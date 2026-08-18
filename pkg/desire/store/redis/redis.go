package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

// maxCASRetries bounds WATCH/MULTI retries when a watched key changes underfoot.
const maxCASRetries = 16

// scanCount is the COUNT hint passed to Redis SCAN, bounding the keys returned
// per page.
const scanCount int64 = 100

// errCASNoop aborts a WATCH callback without writing (no matching mutation).
var errCASNoop = errors.New("desire: cas noop")

// Client is the Redis surface this store needs: Cmdable plus Watch for
// optimistic transactions (WATCH/MULTI/EXEC).
type Client interface {
	redis.Cmdable
	Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error
}

// resourceRecord is the JSON-serialized record. A nil Apply marks the absence
// of the apply sub-state; Delete and Read track their own. Statuses are values.
type resourceRecord struct {
	Apply            *desire.ApplySpec  `json:"apply,omitempty"`
	Key              desire.ResourceKey `json:"key"`
	ApplyResourceID  string             `json:"applyResourceId,omitempty"`
	DeleteResourceID string             `json:"deleteResourceId,omitempty"`
	ReadResourceID   string             `json:"readResourceId,omitempty"`
	Owner            string             `json:"owner"`
	ReadStatus       desire.ReadStatus  `json:"readStatus"`
	ApplyStatus      desire.Status      `json:"applyStatus"`
	DeleteStatus     desire.Status      `json:"deleteStatus"`
	Version          int64              `json:"version"`
	Delete           bool               `json:"delete,omitempty"`
	Read             bool               `json:"read,omitempty"`
}

// Store is a Redis-backed implementation of SpecStore and StatusStore.
//
// Mutating operations use Redis WATCH/MULTI/EXEC so version checks and writes
// are linearizable under concurrent clients (compare-and-swap on Version).
type Store struct {
	client Client
}

// New creates a new Redis store connected to the given client.
func New(client Client) *Store {
	return &Store{client: client}
}

// loadRecord fetches and decodes the record at key, mapping a missing key to
// desire.ErrNotFound.
func (s *Store) loadRecord(ctx context.Context, key string) (*resourceRecord, error) {
	data, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, desire.ErrNotFound
		}
		return nil, fmt.Errorf("desire: get %s: %w", key, err)
	}
	var rec resourceRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("desire: unmarshal %s: %w", key, err)
	}
	return &rec, nil
}

// casMutate runs mutate under WATCH/MULTI/EXEC. TxFailedErr retries up to
// maxCASRetries, then returns ErrAborted. errCASNoop yields (nil, nil).
func (s *Store) casMutate(
	ctx context.Context, key string, mutate func(rec *resourceRecord, exists bool) error,
) (*resourceRecord, error) {
	var out *resourceRecord
	for attempt := 1; attempt <= maxCASRetries; attempt++ {
		err := s.client.Watch(ctx, func(tx *redis.Tx) error {
			data, getErr := tx.Get(ctx, key).Bytes()
			exists := true
			var rec resourceRecord
			switch {
			case errors.Is(getErr, redis.Nil):
				exists = false
			case getErr != nil:
				return fmt.Errorf("desire: get %s: %w", key, getErr)
			default:
				if uErr := json.Unmarshal(data, &rec); uErr != nil {
					return fmt.Errorf("desire: unmarshal %s: %w", key, uErr)
				}
			}

			if mErr := mutate(&rec, exists); mErr != nil {
				return mErr
			}

			_, pErr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				if rec.isEmpty() {
					pipe.Del(ctx, key)
					return nil
				}
				b, mErr := json.Marshal(&rec)
				if mErr != nil {
					return fmt.Errorf("desire: marshal %s: %w", key, mErr)
				}
				pipe.Set(ctx, key, b, 0)
				return nil
			})
			if pErr != nil {
				return pErr
			}
			out = &rec
			return nil
		}, key)

		switch {
		case err == nil:
			return out, nil
		case errors.Is(err, errCASNoop):
			return nil, nil
		case errors.Is(err, redis.TxFailedErr):
			continue
		default:
			return nil, err
		}
	}
	return nil, desire.ErrAborted
}

// CreateApplyDesire creates a new ApplyDesire.
func (s *Store) CreateApplyDesire(ctx context.Context, d desire.ApplyDesire) (desire.ApplyDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.ApplyDesire{}, fmt.Errorf("desire: validate apply desire: %w", err)
	}

	rk := d.Identity.ResourceKey()
	key := rk.String()

	rec, err := s.casMutate(ctx, key, func(rec *resourceRecord, exists bool) error {
		if exists {
			if err := desire.CheckOwner(ctx, rk, rec.Owner, d.Owner); err != nil {
				return fmt.Errorf("desire: create apply desire %s: %w", rk, err)
			}
			if rec.Delete {
				return desire.ErrDeletePending
			}
			if rec.Apply != nil {
				return desire.ErrAlreadyExists
			}
		} else {
			rec.Key = rk
			rec.Owner = d.Owner
		}
		spec := d.Spec
		rec.Apply = &spec
		rec.ApplyResourceID = d.ResourceID
		rec.ApplyStatus = desire.Status{}
		rec.Version++
		return nil
	})
	if err != nil {
		return desire.ApplyDesire{}, err
	}
	return s.projectApplyDesire(rec), nil
}

// GetApplyDesire retrieves an ApplyDesire.
func (s *Store) GetApplyDesire(ctx context.Context, id desire.Identity) (desire.ApplyDesire, error) {
	rk := id.ResourceKey()
	key := rk.String()

	rec, err := s.loadRecord(ctx, key)
	if err != nil {
		return desire.ApplyDesire{}, err
	}

	if rec.Apply == nil {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}

	return s.projectApplyDesire(rec), nil
}

// UpdateApplyDesireSpec updates the spec of an ApplyDesire.
func (s *Store) UpdateApplyDesireSpec(
	ctx context.Context, id desire.Identity, spec desire.ApplySpec, owner string, version int64,
) (desire.ApplyDesire, error) {
	if err := spec.Validate(); err != nil {
		return desire.ApplyDesire{}, fmt.Errorf("desire: validate apply spec: %w", err)
	}

	rk := id.ResourceKey()
	key := rk.String()

	rec, err := s.casMutate(ctx, key, func(rec *resourceRecord, exists bool) error {
		if !exists || rec.Apply == nil {
			return desire.ErrNotFound
		}
		if rec.Version != version {
			return desire.ErrVersionConflict
		}
		if err := desire.CheckOwner(ctx, rk, rec.Owner, owner); err != nil {
			return fmt.Errorf("desire: update apply desire spec %s: %w", rk, err)
		}
		rec.Apply = &spec
		// Clear the status: it described the previous spec, which is no longer
		// the desired state. Retaining a stale Successful=True would report the
		// new, unreconciled spec as already achieved. Matches Create/Delete,
		// which also reset ApplyStatus.
		rec.ApplyStatus = desire.Status{}
		rec.Version++
		return nil
	})
	if err != nil {
		return desire.ApplyDesire{}, err
	}
	return s.projectApplyDesire(rec), nil
}

// DeleteApplyDesire deletes an ApplyDesire.
func (s *Store) DeleteApplyDesire(ctx context.Context, id desire.Identity, owner string, version int64) error {
	rk := id.ResourceKey()
	key := rk.String()

	_, err := s.casMutate(ctx, key, func(rec *resourceRecord, exists bool) error {
		if !exists || rec.Apply == nil {
			return desire.ErrNotFound
		}
		if rec.Version != version {
			return desire.ErrVersionConflict
		}
		if err := desire.CheckOwner(ctx, rk, rec.Owner, owner); err != nil {
			return fmt.Errorf("desire: delete apply desire %s: %w", rk, err)
		}
		rec.Apply = nil
		rec.ApplyResourceID = ""
		rec.ApplyStatus = desire.Status{}
		rec.Version++
		return nil
	})
	return err
}

// CreateDeleteDesire creates a new DeleteDesire.
func (s *Store) CreateDeleteDesire(ctx context.Context, d desire.DeleteDesire) (desire.DeleteDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.DeleteDesire{}, fmt.Errorf("desire: validate delete desire: %w", err)
	}

	rk := d.Identity.ResourceKey()
	key := rk.String()

	rec, err := s.casMutate(ctx, key, func(rec *resourceRecord, exists bool) error {
		if exists {
			if err := desire.CheckOwner(ctx, rk, rec.Owner, d.Owner); err != nil {
				return fmt.Errorf("desire: create delete desire %s: %w", rk, err)
			}
			if rec.Delete {
				return desire.ErrAlreadyExists
			}
		} else {
			rec.Key = rk
			rec.Owner = d.Owner
		}
		// Delete supersedes an existing Apply.
		rec.Apply = nil
		rec.ApplyResourceID = ""
		rec.ApplyStatus = desire.Status{}
		rec.Delete = true
		rec.DeleteResourceID = d.ResourceID
		rec.DeleteStatus = desire.Status{}
		rec.Version++
		return nil
	})
	if err != nil {
		return desire.DeleteDesire{}, err
	}
	return s.projectDeleteDesire(rec), nil
}

// GetDeleteDesire retrieves a DeleteDesire.
func (s *Store) GetDeleteDesire(ctx context.Context, id desire.Identity) (desire.DeleteDesire, error) {
	rk := id.ResourceKey()
	key := rk.String()

	rec, err := s.loadRecord(ctx, key)
	if err != nil {
		return desire.DeleteDesire{}, err
	}

	if !rec.Delete {
		return desire.DeleteDesire{}, desire.ErrNotFound
	}

	return s.projectDeleteDesire(rec), nil
}

// DeleteDeleteDesire deletes a DeleteDesire.
func (s *Store) DeleteDeleteDesire(ctx context.Context, id desire.Identity, owner string, version int64) error {
	rk := id.ResourceKey()
	key := rk.String()

	_, err := s.casMutate(ctx, key, func(rec *resourceRecord, exists bool) error {
		if !exists || !rec.Delete {
			return desire.ErrNotFound
		}
		if rec.Version != version {
			return desire.ErrVersionConflict
		}
		if err := desire.CheckOwner(ctx, rk, rec.Owner, owner); err != nil {
			return fmt.Errorf("desire: delete delete desire %s: %w", rk, err)
		}
		rec.Delete = false
		rec.DeleteResourceID = ""
		rec.DeleteStatus = desire.Status{}
		rec.Version++
		return nil
	})
	return err
}

// CreateReadDesire creates a new ReadDesire.
func (s *Store) CreateReadDesire(ctx context.Context, d desire.ReadDesire) (desire.ReadDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.ReadDesire{}, fmt.Errorf("desire: validate read desire: %w", err)
	}

	rk := d.Identity.ResourceKey()
	key := rk.String()

	rec, err := s.casMutate(ctx, key, func(rec *resourceRecord, exists bool) error {
		if exists {
			if err := desire.CheckOwner(ctx, rk, rec.Owner, d.Owner); err != nil {
				return fmt.Errorf("desire: create read desire %s: %w", rk, err)
			}
			if rec.Read {
				return desire.ErrAlreadyExists
			}
		} else {
			rec.Key = rk
			rec.Owner = d.Owner
		}
		rec.Read = true
		rec.ReadResourceID = d.ResourceID
		rec.ReadStatus = desire.ReadStatus{}
		rec.Version++
		return nil
	})
	if err != nil {
		return desire.ReadDesire{}, err
	}
	return s.projectReadDesire(rec), nil
}

// GetReadDesire retrieves a ReadDesire.
func (s *Store) GetReadDesire(ctx context.Context, id desire.Identity) (desire.ReadDesire, error) {
	rk := id.ResourceKey()
	key := rk.String()

	rec, err := s.loadRecord(ctx, key)
	if err != nil {
		return desire.ReadDesire{}, err
	}

	if !rec.Read {
		return desire.ReadDesire{}, desire.ErrNotFound
	}

	return s.projectReadDesire(rec), nil
}

// DeleteReadDesire deletes a ReadDesire.
func (s *Store) DeleteReadDesire(ctx context.Context, id desire.Identity, owner string, version int64) error {
	rk := id.ResourceKey()
	key := rk.String()

	_, err := s.casMutate(ctx, key, func(rec *resourceRecord, exists bool) error {
		if !exists || !rec.Read {
			return desire.ErrNotFound
		}
		if rec.Version != version {
			return desire.ErrVersionConflict
		}
		if err := desire.CheckOwner(ctx, rk, rec.Owner, owner); err != nil {
			return fmt.Errorf("desire: delete read desire %s: %w", rk, err)
		}
		rec.Read = false
		rec.ReadResourceID = ""
		rec.ReadStatus = desire.ReadStatus{}
		rec.Version++
		return nil
	})
	return err
}

// clusterRecord pairs a decoded record with the Redis key it was loaded from.
type clusterRecord struct {
	Record   *resourceRecord
	RedisKey string
}

// loadClusterRecords fetches and decodes all records for a management
// cluster via SCAN + MGET, instead of one GET per key.
func (s *Store) loadClusterRecords(ctx context.Context, managementCluster string) ([]clusterRecord, error) {
	pattern := desire.ManagementClusterKeyPrefix(managementCluster) + "*"
	keys, err := s.scanKeys(ctx, pattern)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}

	values := make([]any, len(keys))
	const mgetBatch = 1000
	for start := 0; start < len(keys); start += mgetBatch {
		end := min(start+mgetBatch, len(keys))
		batch, err := s.client.MGet(ctx, keys[start:end]...).Result()
		if err != nil {
			return nil, fmt.Errorf("desire: mget: %w", err)
		}
		copy(values[start:end], batch)
	}

	records := make([]clusterRecord, 0, len(values))
	for i, v := range values {
		if v == nil {
			// Key vanished between SCAN and MGET; skip.
			continue
		}
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("desire: unexpected value type for key %q: %T", keys[i], v)
		}
		var rec resourceRecord
		if err := json.Unmarshal([]byte(str), &rec); err != nil {
			return nil, fmt.Errorf("desire: decoding record %q: %w", keys[i], err)
		}
		records = append(records, clusterRecord{RedisKey: keys[i], Record: &rec})
	}
	return records, nil
}

// scanKeys collects unique Redis keys matching pattern using incremental SCAN.
// SCAN may emit the same key more than once; duplicates are dropped.
func (s *Store) scanKeys(ctx context.Context, pattern string) ([]string, error) {
	var (
		cursor uint64
		keys   []string
		seen   = make(map[string]struct{})
	)
	for {
		batch, next, err := s.client.Scan(ctx, cursor, pattern, scanCount).Result()
		if err != nil {
			return nil, fmt.Errorf("desire: scan %s: %w", pattern, err)
		}
		for _, k := range batch {
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
		cursor = next
		if cursor == 0 {
			return keys, nil
		}
	}
}

// ListApplyDesires returns all ApplyDesires for a management cluster.
func (s *Store) ListApplyDesires(ctx context.Context, managementCluster string) ([]desire.ApplyDesire, error) {
	records, err := s.loadClusterRecords(ctx, managementCluster)
	if err != nil {
		return nil, err
	}

	result := make([]desire.ApplyDesire, 0, len(records))
	for _, cr := range records {
		if cr.Record.Apply != nil {
			result = append(result, s.projectApplyDesire(cr.Record))
		}
	}
	return result, nil
}

// ListDeleteDesires returns all DeleteDesires for a management cluster.
func (s *Store) ListDeleteDesires(ctx context.Context, managementCluster string) ([]desire.DeleteDesire, error) {
	records, err := s.loadClusterRecords(ctx, managementCluster)
	if err != nil {
		return nil, err
	}

	result := make([]desire.DeleteDesire, 0, len(records))
	for _, cr := range records {
		if cr.Record.Delete {
			result = append(result, s.projectDeleteDesire(cr.Record))
		}
	}
	return result, nil
}

// ListReadDesires returns all ReadDesires for a management cluster.
func (s *Store) ListReadDesires(ctx context.Context, managementCluster string) ([]desire.ReadDesire, error) {
	records, err := s.loadClusterRecords(ctx, managementCluster)
	if err != nil {
		return nil, err
	}

	result := make([]desire.ReadDesire, 0, len(records))
	for _, cr := range records {
		if cr.Record.Read {
			result = append(result, s.projectReadDesire(cr.Record))
		}
	}
	return result, nil
}

// DeleteByPrefix deletes desires matching a selector within a management cluster.
func (s *Store) DeleteByPrefix(ctx context.Context, managementCluster string, sel desire.PrefixSelector) error {
	records, err := s.loadClusterRecords(ctx, managementCluster)
	if err != nil {
		return err
	}

	for _, cr := range records {
		rec := cr.Record
		needsWrite := (rec.Apply != nil && sel.Matches(rec.Key.Identity(desire.TypeApply))) ||
			(rec.Delete && sel.Matches(rec.Key.Identity(desire.TypeDelete))) ||
			(rec.Read && sel.Matches(rec.Key.Identity(desire.TypeRead)))
		if !needsWrite {
			continue
		}

		_, err := s.casMutate(ctx, cr.RedisKey, func(rec *resourceRecord, exists bool) error {
			if !exists {
				return errCASNoop
			}

			modified := false
			if rec.Apply != nil && sel.Matches(rec.Key.Identity(desire.TypeApply)) {
				rec.Apply = nil
				rec.ApplyResourceID = ""
				rec.ApplyStatus = desire.Status{}
				modified = true
			}
			if rec.Delete && sel.Matches(rec.Key.Identity(desire.TypeDelete)) {
				rec.Delete = false
				rec.DeleteResourceID = ""
				rec.DeleteStatus = desire.Status{}
				modified = true
			}
			if rec.Read && sel.Matches(rec.Key.Identity(desire.TypeRead)) {
				rec.Read = false
				rec.ReadResourceID = ""
				rec.ReadStatus = desire.ReadStatus{}
				modified = true
			}
			if !modified {
				return errCASNoop
			}
			rec.Version++
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// UpdateApplyDesireStatus updates the status of an ApplyDesire.
func (s *Store) UpdateApplyDesireStatus(
	ctx context.Context, id desire.Identity, status desire.Status, version int64,
) (desire.ApplyDesire, error) {
	rk := id.ResourceKey()
	key := rk.String()

	rec, err := s.casMutate(ctx, key, func(rec *resourceRecord, exists bool) error {
		if !exists || rec.Apply == nil {
			return desire.ErrNotFound
		}
		if rec.Version != version {
			return desire.ErrVersionConflict
		}
		rec.ApplyStatus = desire.CloneStatus(status)
		rec.Version++
		return nil
	})
	if err != nil {
		return desire.ApplyDesire{}, err
	}
	return s.projectApplyDesire(rec), nil
}

// UpdateDeleteDesireStatus updates the status of a DeleteDesire.
func (s *Store) UpdateDeleteDesireStatus(
	ctx context.Context, id desire.Identity, status desire.Status, version int64,
) (desire.DeleteDesire, error) {
	rk := id.ResourceKey()
	key := rk.String()

	rec, err := s.casMutate(ctx, key, func(rec *resourceRecord, exists bool) error {
		if !exists || !rec.Delete {
			return desire.ErrNotFound
		}
		if rec.Version != version {
			return desire.ErrVersionConflict
		}
		rec.DeleteStatus = desire.CloneStatus(status)
		rec.Version++
		return nil
	})
	if err != nil {
		return desire.DeleteDesire{}, err
	}
	return s.projectDeleteDesire(rec), nil
}

// UpdateReadDesireStatus updates the status of a ReadDesire.
func (s *Store) UpdateReadDesireStatus(
	ctx context.Context, id desire.Identity, status desire.ReadStatus,
) (desire.ReadDesire, error) {
	rk := id.ResourceKey()
	key := rk.String()

	rec, err := s.casMutate(ctx, key, func(rec *resourceRecord, exists bool) error {
		if !exists || !rec.Read {
			return desire.ErrNotFound
		}
		rec.ReadStatus = desire.CloneReadStatus(status)
		return nil
	})
	if err != nil {
		return desire.ReadDesire{}, err
	}
	return s.projectReadDesire(rec), nil
}

// Helper methods to project records into public types.

func (s *Store) projectApplyDesire(rec *resourceRecord) desire.ApplyDesire {
	if rec == nil || rec.Apply == nil {
		return desire.ApplyDesire{}
	}
	return desire.ApplyDesire{
		Identity:   rec.Key.Identity(desire.TypeApply),
		Owner:      rec.Owner,
		ResourceID: rec.ApplyResourceID,
		Version:    rec.Version,
		Spec:       desire.CloneApplySpec(*rec.Apply),
		Status:     desire.CloneStatus(rec.ApplyStatus),
	}
}

func (s *Store) projectDeleteDesire(rec *resourceRecord) desire.DeleteDesire {
	if rec == nil {
		return desire.DeleteDesire{}
	}
	return desire.DeleteDesire{
		Identity:   rec.Key.Identity(desire.TypeDelete),
		Owner:      rec.Owner,
		ResourceID: rec.DeleteResourceID,
		Version:    rec.Version,
		Status:     desire.CloneStatus(rec.DeleteStatus),
	}
}

func (s *Store) projectReadDesire(rec *resourceRecord) desire.ReadDesire {
	if rec == nil {
		return desire.ReadDesire{}
	}
	return desire.ReadDesire{
		Identity:   rec.Key.Identity(desire.TypeRead),
		Owner:      rec.Owner,
		ResourceID: rec.ReadResourceID,
		Version:    rec.Version,
		Status:     desire.CloneReadStatus(rec.ReadStatus),
	}
}

// isEmpty reports whether this record has no active sub-states.
func (rec *resourceRecord) isEmpty() bool {
	return rec.Apply == nil && !rec.Delete && !rec.Read
}
