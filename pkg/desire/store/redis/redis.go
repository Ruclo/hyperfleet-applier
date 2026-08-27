// Package redis provides a Redis-backed desire store.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"strings"
	"time"

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

// keyPrefix starts every desire key and opens the Redis Cluster hash tag.
const keyPrefix = "desire/{"

// Client is the Redis surface this store needs: Cmdable plus Watch for
// optimistic transactions (WATCH/MULTI/EXEC).
type Client interface {
	redis.Cmdable
	Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error
}

// resourceRecord is one serialized desire keyed by full Identity.
// Apply uses Apply+Status, Delete uses Status, and Read uses ReadStatus.
type resourceRecord struct {
	Apply         *desire.ApplySpec `json:"apply,omitempty"`
	Identity      desire.Identity   `json:"identity"`
	OriginID      string            `json:"originId,omitempty"`
	Owner         string            `json:"owner"`
	TargetVersion string            `json:"targetVersion,omitempty"`
	ReadStatus    desire.ReadStatus `json:"readStatus"`
	Status        desire.Status     `json:"status"`
	Version       int64             `json:"version"`
}

// Store is a Redis-backed SpecStore and StatusStore.
// Mutations use WATCH/MULTI/EXEC for versioned compare-and-swap.
type Store struct {
	client Client
}

// New creates a new Redis store connected to the given client.
func New(client Client) *Store {
	return &Store{client: client}
}

// redisKey encodes an Identity into a Redis key.
// The target coordinates share one hash tag so sibling desire keys land in the
// same cluster slot; fields are path-escaped.
func redisKey(id desire.Identity) string {
	return keyPrefix + strings.Join([]string{
		url.PathEscape(id.ManagementCluster),
		url.PathEscape(id.Group),
		url.PathEscape(id.Resource),
		url.PathEscape(id.Namespace),
		url.PathEscape(id.Name),
	}, "/") + "}/" + string(id.Type)
}

// globEscaper escapes Redis glob metacharacters so a key fragment is matched
var globEscaper = strings.NewReplacer(
	`\`, `\\`, `*`, `\*`, `?`, `\?`, `[`, `\[`, `]`, `\]`,
)

// mcScanPrefix returns the Redis SCAN prefix for a management cluster.
func mcScanPrefix(managementCluster string) string {
	return globEscaper.Replace(keyPrefix+url.PathEscape(managementCluster)) + "/"
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

// getRecordTx fetches and decodes the record at key within a transaction,
// returning (nil, nil) when the key is absent.
func getRecordTx(ctx context.Context, tx *redis.Tx, key string) (*resourceRecord, error) {
	data, err := tx.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("desire: get %s: %w", key, err)
	}
	var rec resourceRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("desire: unmarshal %s: %w", key, err)
	}
	return &rec, nil
}

// casBaseDelay is the base jitter ceiling for CAS retry backoff.
const casBaseDelay = 5 * time.Millisecond

// watchRetry runs fn under WATCH/MULTI/EXEC and retries TxFailedErr
// with a short randomized, increasing delay between attempts.
// errCASNoop counts as success; retries stop at ErrAborted.
func (s *Store) watchRetry(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error {
	for attempt := 1; attempt <= maxCASRetries; attempt++ {
		err := s.client.Watch(ctx, fn, keys...)
		switch {
		case err == nil, errors.Is(err, errCASNoop):
			return nil
		case errors.Is(err, redis.TxFailedErr):
			ceil := int64(casBaseDelay) * int64(attempt)
			d := rand.Int64N(ceil) //nolint:gosec // jitter
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(d)):
			}
		default:
			return err
		}
	}
	return desire.ErrAborted
}

// casMutate loads a record, lets mutate update it, then writes it back.
// errCASNoop returns (nil, nil).
func (s *Store) casMutate(
	ctx context.Context, key string, mutate func(rec *resourceRecord, exists bool) error,
) (*resourceRecord, error) {
	var out *resourceRecord
	err := s.watchRetry(ctx, func(tx *redis.Tx) error {
		rec, getErr := getRecordTx(ctx, tx, key)
		if getErr != nil {
			return getErr
		}
		exists := rec != nil
		if rec == nil {
			rec = &resourceRecord{}
		}
		if mErr := mutate(rec, exists); mErr != nil {
			return mErr
		}
		_, pErr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			b, mErr := json.Marshal(rec)
			if mErr != nil {
				return fmt.Errorf("desire: marshal %s: %w", key, mErr)
			}
			pipe.Set(ctx, key, b, 0)
			return nil
		})
		if pErr != nil {
			return pErr
		}
		out = rec
		return nil
	}, key)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// casDelete loads a record, runs precheck, then deletes it.
// precheck may return errCASNoop to skip the delete.
func (s *Store) casDelete(
	ctx context.Context, key string, precheck func(rec *resourceRecord, exists bool) error,
) error {
	return s.watchRetry(ctx, func(tx *redis.Tx) error {
		rec, getErr := getRecordTx(ctx, tx, key)
		if getErr != nil {
			return getErr
		}
		exists := rec != nil
		if rec == nil {
			rec = &resourceRecord{}
		}
		if pErr := precheck(rec, exists); pErr != nil {
			return pErr
		}
		_, pErr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, key)
			return nil
		})
		return pErr
	}, key)
}

// targetSiblings maps existing sibling desire records by type.
type targetSiblings = map[desire.DesireType]*resourceRecord

// casCreate watches all sibling desire keys for a target and creates one record
// atomically, optionally deleting sibling keys in the same transaction.
func (s *Store) casCreate(
	ctx context.Context,
	id desire.Identity,
	build func(sibs targetSiblings) (*resourceRecord, []string, error),
) (*resourceRecord, error) {
	keyOf := func(t desire.DesireType) string {
		sib := id
		sib.Type = t
		return redisKey(sib)
	}
	keys := make([]string, len(desire.AllTypes()))
	for i, t := range desire.AllTypes() {
		keys[i] = keyOf(t)
	}

	var out *resourceRecord
	err := s.watchRetry(ctx, func(tx *redis.Tx) error {
		sibs := make(targetSiblings, len(desire.AllTypes()))
		for _, t := range desire.AllTypes() {
			rec, getErr := getRecordTx(ctx, tx, keyOf(t))
			if getErr != nil {
				return getErr
			}
			if rec != nil {
				sibs[t] = rec
			}
		}

		newRec, delKeys, bErr := build(sibs)
		if bErr != nil {
			return bErr
		}

		newKey := redisKey(newRec.Identity)
		_, pErr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			b, mErr := json.Marshal(newRec)
			if mErr != nil {
				return fmt.Errorf("desire: marshal %s: %w", newKey, mErr)
			}
			pipe.Set(ctx, newKey, b, 0)
			for _, dk := range delKeys {
				pipe.Del(ctx, dk)
			}
			return nil
		})
		if pErr != nil {
			return pErr
		}
		out = newRec
		return nil
	}, keys...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// checkTargetOwner enforces the shared owner for a target.
func checkTargetOwner(
	ctx context.Context, id desire.Identity, attempted string, sibs targetSiblings,
) error {
	for _, t := range desire.AllTypes() {
		if r := sibs[t]; r != nil {
			return desire.CheckOwner(ctx, id, r.Owner, attempted)
		}
	}
	return nil
}

// CreateApplyDesire creates a new ApplyDesire.
func (s *Store) CreateApplyDesire(ctx context.Context, d desire.ApplyDesire) (desire.ApplyDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.ApplyDesire{}, fmt.Errorf("desire: validate apply desire: %w", err)
	}

	rec, err := s.casCreate(ctx, d.Identity, func(sibs targetSiblings) (*resourceRecord, []string, error) {
		if err := checkTargetOwner(ctx, d.Identity, d.Owner, sibs); err != nil {
			return nil, nil, fmt.Errorf("desire: create apply desire %s: %w", d.Identity, err)
		}
		spec := desire.CloneApplySpec(d.Spec)
		if deleted := sibs[desire.TypeDelete]; deleted != nil {
			if !desire.IsDeleted(deleted.Status) {
				return nil, nil, desire.ErrDeletePending
			}
			// Retire the completed delete atomically with the new apply.
			return &resourceRecord{
				Identity: d.Identity,
				Owner:    d.Owner,
				OriginID: d.OriginID,
				Version:  1,
				Apply:    &spec,
			}, []string{redisKey(deleted.Identity)}, nil
		}
		if sibs[desire.TypeApply] != nil {
			return nil, nil, desire.ErrAlreadyExists
		}
		return &resourceRecord{
			Identity: d.Identity,
			Owner:    d.Owner,
			OriginID: d.OriginID,
			Version:  1,
			Apply:    &spec,
		}, nil, nil
	})
	if err != nil {
		return desire.ApplyDesire{}, err
	}
	return s.projectApplyDesire(rec), nil
}

// GetApplyDesire retrieves an ApplyDesire.
func (s *Store) GetApplyDesire(ctx context.Context, id desire.Identity) (desire.ApplyDesire, error) {
	rec, err := s.loadRecord(ctx, redisKey(id))
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

	rec, err := s.casMutate(ctx, redisKey(id), func(rec *resourceRecord, exists bool) error {
		if !exists || rec.Apply == nil {
			return desire.ErrNotFound
		}
		if rec.Version != version {
			return desire.ErrVersionConflict
		}
		if err := desire.CheckOwner(ctx, id, rec.Owner, owner); err != nil {
			return fmt.Errorf("desire: update apply desire spec %s: %w", id, err)
		}
		cloned := desire.CloneApplySpec(spec)
		rec.Apply = &cloned
		// Clear the status: it described the previous spec, which is no longer
		// the desired state. Retaining a stale Successful=True would report the
		// new, unreconciled spec as already achieved. Matches Create/Delete,
		// which also reset the status.
		rec.Status = desire.Status{}
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
	return s.casDelete(ctx, redisKey(id), func(rec *resourceRecord, exists bool) error {
		if !exists || rec.Apply == nil {
			return desire.ErrNotFound
		}
		if rec.Version != version {
			return desire.ErrVersionConflict
		}
		if err := desire.CheckOwner(ctx, id, rec.Owner, owner); err != nil {
			return fmt.Errorf("desire: delete apply desire %s: %w", id, err)
		}
		return nil
	})
}

// CreateDeleteDesire creates a new DeleteDesire.
func (s *Store) CreateDeleteDesire(ctx context.Context, d desire.DeleteDesire) (desire.DeleteDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.DeleteDesire{}, fmt.Errorf("desire: validate delete desire: %w", err)
	}

	rec, err := s.casCreate(ctx, d.Identity, func(sibs targetSiblings) (*resourceRecord, []string, error) {
		if err := checkTargetOwner(ctx, d.Identity, d.Owner, sibs); err != nil {
			return nil, nil, fmt.Errorf("desire: create delete desire %s: %w", d.Identity, err)
		}
		if sibs[desire.TypeDelete] != nil {
			return nil, nil, desire.ErrAlreadyExists
		}
		// Delete supersedes an existing Apply for the same target.
		var delKeys []string
		if apply := sibs[desire.TypeApply]; apply != nil {
			delKeys = append(delKeys, redisKey(apply.Identity))
		}
		return &resourceRecord{
			Identity: d.Identity,
			Owner:    d.Owner,
			OriginID: d.OriginID,
			Version:  1,
		}, delKeys, nil
	})
	if err != nil {
		return desire.DeleteDesire{}, err
	}
	return s.projectDeleteDesire(rec), nil
}

// GetDeleteDesire retrieves a DeleteDesire.
func (s *Store) GetDeleteDesire(ctx context.Context, id desire.Identity) (desire.DeleteDesire, error) {
	if id.Type != desire.TypeDelete {
		return desire.DeleteDesire{}, desire.ErrNotFound
	}
	rec, err := s.loadRecord(ctx, redisKey(id))
	if err != nil {
		return desire.DeleteDesire{}, err
	}
	return s.projectDeleteDesire(rec), nil
}

// DeleteDeleteDesire deletes a DeleteDesire.
func (s *Store) DeleteDeleteDesire(ctx context.Context, id desire.Identity, owner string, version int64) error {
	if id.Type != desire.TypeDelete {
		return desire.ErrNotFound
	}
	return s.casDelete(ctx, redisKey(id), func(rec *resourceRecord, exists bool) error {
		if !exists {
			return desire.ErrNotFound
		}
		if rec.Version != version {
			return desire.ErrVersionConflict
		}
		if err := desire.CheckOwner(ctx, id, rec.Owner, owner); err != nil {
			return fmt.Errorf("desire: delete delete desire %s: %w", id, err)
		}
		return nil
	})
}

// CreateReadDesire creates a new ReadDesire.
func (s *Store) CreateReadDesire(ctx context.Context, d desire.ReadDesire) (desire.ReadDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.ReadDesire{}, fmt.Errorf("desire: validate read desire: %w", err)
	}

	rec, err := s.casCreate(ctx, d.Identity, func(sibs targetSiblings) (*resourceRecord, []string, error) {
		if err := checkTargetOwner(ctx, d.Identity, d.Owner, sibs); err != nil {
			return nil, nil, fmt.Errorf("desire: create read desire %s: %w", d.Identity, err)
		}
		if sibs[desire.TypeRead] != nil {
			return nil, nil, desire.ErrAlreadyExists
		}
		return &resourceRecord{
			Identity:      d.Identity,
			Owner:         d.Owner,
			OriginID:      d.OriginID,
			TargetVersion: d.TargetVersion,
			Version:       1,
		}, nil, nil
	})
	if err != nil {
		return desire.ReadDesire{}, err
	}
	return s.projectReadDesire(rec), nil
}

// GetReadDesire retrieves a ReadDesire.
func (s *Store) GetReadDesire(ctx context.Context, id desire.Identity) (desire.ReadDesire, error) {
	if id.Type != desire.TypeRead {
		return desire.ReadDesire{}, desire.ErrNotFound
	}
	rec, err := s.loadRecord(ctx, redisKey(id))
	if err != nil {
		return desire.ReadDesire{}, err
	}
	return s.projectReadDesire(rec), nil
}

// DeleteReadDesire deletes a ReadDesire.
func (s *Store) DeleteReadDesire(ctx context.Context, id desire.Identity, owner string, version int64) error {
	if id.Type != desire.TypeRead {
		return desire.ErrNotFound
	}
	return s.casDelete(ctx, redisKey(id), func(rec *resourceRecord, exists bool) error {
		if !exists {
			return desire.ErrNotFound
		}
		if rec.Version != version {
			return desire.ErrVersionConflict
		}
		if err := desire.CheckOwner(ctx, id, rec.Owner, owner); err != nil {
			return fmt.Errorf("desire: delete read desire %s: %w", id, err)
		}
		return nil
	})
}

// clusterRecord pairs a decoded record with the Redis key it was loaded from.
type clusterRecord struct {
	Record   *resourceRecord
	RedisKey string
}

// loadClusterRecords fetches and decodes all records for a management
// cluster via SCAN + MGET, instead of one GET per key.
func (s *Store) loadClusterRecords(ctx context.Context, managementCluster string) ([]clusterRecord, error) {
	pattern := mcScanPrefix(managementCluster) + "*"
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
		if cr.Record.Identity.Type == desire.TypeApply {
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
		if cr.Record.Identity.Type == desire.TypeDelete {
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
		if cr.Record.Identity.Type == desire.TypeRead {
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
		if !sel.Matches(cr.Record.Identity) {
			continue
		}
		err := s.casDelete(ctx, cr.RedisKey, func(rec *resourceRecord, exists bool) error {
			if !exists || !sel.Matches(rec.Identity) {
				return errCASNoop
			}
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
	rec, err := s.casMutate(ctx, redisKey(id), func(rec *resourceRecord, exists bool) error {
		if !exists || rec.Apply == nil {
			return desire.ErrNotFound
		}
		if rec.Version != version {
			return desire.ErrVersionConflict
		}
		rec.Status = desire.CloneStatus(status)
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
	if id.Type != desire.TypeDelete {
		return desire.DeleteDesire{}, desire.ErrNotFound
	}
	rec, err := s.casMutate(ctx, redisKey(id), func(rec *resourceRecord, exists bool) error {
		if !exists {
			return desire.ErrNotFound
		}
		if rec.Version != version {
			return desire.ErrVersionConflict
		}
		rec.Status = desire.CloneStatus(status)
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
	if id.Type != desire.TypeRead {
		return desire.ReadDesire{}, desire.ErrNotFound
	}
	rec, err := s.casMutate(ctx, redisKey(id), func(rec *resourceRecord, exists bool) error {
		if !exists {
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
		Identity: rec.Identity,
		Owner:    rec.Owner,
		OriginID: rec.OriginID,
		Version:  rec.Version,
		Spec:     desire.CloneApplySpec(*rec.Apply),
		Status:   desire.CloneStatus(rec.Status),
	}
}

func (s *Store) projectDeleteDesire(rec *resourceRecord) desire.DeleteDesire {
	if rec == nil {
		return desire.DeleteDesire{}
	}
	return desire.DeleteDesire{
		Identity: rec.Identity,
		Owner:    rec.Owner,
		OriginID: rec.OriginID,
		Version:  rec.Version,
		Status:   desire.CloneStatus(rec.Status),
	}
}

func (s *Store) projectReadDesire(rec *resourceRecord) desire.ReadDesire {
	if rec == nil {
		return desire.ReadDesire{}
	}
	return desire.ReadDesire{
		Identity:      rec.Identity,
		Owner:         rec.Owner,
		OriginID:      rec.OriginID,
		TargetVersion: rec.TargetVersion,
		Version:       rec.Version,
		Status:        desire.CloneReadStatus(rec.ReadStatus),
	}
}
