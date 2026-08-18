package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

// resourceRecord is the internal record structure for a single resource,
// holding at most one of Apply/Delete, with Read independent.
type resourceRecord struct {
	Apply            *desire.ApplySpec
	Key              desire.ResourceKey
	ApplyResourceID  string
	DeleteResourceID string
	ReadResourceID   string
	Owner            string
	ReadStatus       desire.ReadStatus
	ApplyStatus      desire.Status
	DeleteStatus     desire.Status
	Version          int64
	Delete           bool
	Read             bool
}

// Store is a single-process in-memory implementation of SpecStore and StatusStore.
type Store struct {
	items map[string]*resourceRecord
	mu    sync.RWMutex
}

// New creates a new in-memory store.
func New() *Store {
	return &Store{
		items: make(map[string]*resourceRecord),
	}
}

// CreateApplyDesire creates a new ApplyDesire or returns an error.
func (s *Store) CreateApplyDesire(ctx context.Context, d desire.ApplyDesire) (desire.ApplyDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.ApplyDesire{}, fmt.Errorf("desire: validate apply desire: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rk := d.Identity.ResourceKey()
	key := rk.String()
	spec := desire.CloneApplySpec(d.Spec)

	rec, exists := s.items[key]
	if !exists {
		rec = &resourceRecord{
			Key:             rk,
			Owner:           d.Owner,
			Version:         1,
			Apply:           &spec,
			ApplyResourceID: d.ResourceID,
			ApplyStatus:     desire.Status{},
		}
		s.items[key] = rec
		return s.projectApplyDesire(rec), nil
	}

	if err := desire.CheckOwner(ctx, rk, rec.Owner, d.Owner); err != nil {
		return desire.ApplyDesire{}, err
	}
	if rec.Delete {
		return desire.ApplyDesire{}, desire.ErrDeletePending
	}
	if rec.Apply != nil {
		return desire.ApplyDesire{}, desire.ErrAlreadyExists
	}

	rec.Apply = &spec
	rec.ApplyResourceID = d.ResourceID
	rec.ApplyStatus = desire.Status{}
	rec.Version++
	return s.projectApplyDesire(rec), nil
}

// GetApplyDesire retrieves an ApplyDesire or returns ErrNotFound.
func (s *Store) GetApplyDesire(ctx context.Context, id desire.Identity) (desire.ApplyDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rk := id.ResourceKey()
	rec, exists := s.items[rk.String()]
	if !exists || rec.Apply == nil {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}
	return s.projectApplyDesire(rec), nil
}

// UpdateApplyDesireSpec updates the spec and owner of an ApplyDesire.
func (s *Store) UpdateApplyDesireSpec(
	ctx context.Context, id desire.Identity, spec desire.ApplySpec, owner string, version int64,
) (desire.ApplyDesire, error) {
	if err := spec.Validate(); err != nil {
		return desire.ApplyDesire{}, fmt.Errorf("desire: validate apply spec: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rk := id.ResourceKey()
	key := rk.String()

	rec, exists := s.items[key]
	if !exists || rec.Apply == nil {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ApplyDesire{}, desire.ErrVersionConflict
	}
	if err := desire.CheckOwner(ctx, rk, rec.Owner, owner); err != nil {
		return desire.ApplyDesire{}, err
	}

	cloned := desire.CloneApplySpec(spec)
	rec.Apply = &cloned
	// Clear the status: it described the previous spec, which is no longer the
	// desired state. Retaining a stale Successful=True would report the new,
	// unreconciled spec as already achieved. Matches Create/Delete, which also
	// reset ApplyStatus.
	rec.ApplyStatus = desire.Status{}
	rec.Version++
	return s.projectApplyDesire(rec), nil
}

// DeleteApplyDesire deletes an ApplyDesire.
func (s *Store) DeleteApplyDesire(ctx context.Context, id desire.Identity, owner string, version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rk := id.ResourceKey()
	key := rk.String()

	rec, exists := s.items[key]
	if !exists || rec.Apply == nil {
		return desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ErrVersionConflict
	}
	if err := desire.CheckOwner(ctx, rk, rec.Owner, owner); err != nil {
		return err
	}

	rec.Apply = nil
	rec.ApplyResourceID = ""
	rec.ApplyStatus = desire.Status{}
	if rec.isEmpty() {
		delete(s.items, key)
	} else {
		rec.Version++
	}
	return nil
}

// CreateDeleteDesire creates a new DeleteDesire.
func (s *Store) CreateDeleteDesire(ctx context.Context, d desire.DeleteDesire) (desire.DeleteDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.DeleteDesire{}, fmt.Errorf("desire: validate delete desire: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rk := d.Identity.ResourceKey()
	key := rk.String()

	rec, exists := s.items[key]
	if !exists {
		rec = &resourceRecord{
			Key:              rk,
			Owner:            d.Owner,
			Version:          1,
			Delete:           true,
			DeleteResourceID: d.ResourceID,
			DeleteStatus:     desire.Status{},
		}
		s.items[key] = rec
		return s.projectDeleteDesire(rec), nil
	}

	if err := desire.CheckOwner(ctx, rk, rec.Owner, d.Owner); err != nil {
		return desire.DeleteDesire{}, err
	}
	if rec.Delete {
		return desire.DeleteDesire{}, desire.ErrAlreadyExists
	}

	// Delete supersedes an existing Apply.
	rec.Apply = nil
	rec.ApplyResourceID = ""
	rec.ApplyStatus = desire.Status{}
	rec.Delete = true
	rec.DeleteResourceID = d.ResourceID
	rec.DeleteStatus = desire.Status{}
	rec.Version++
	return s.projectDeleteDesire(rec), nil
}

// GetDeleteDesire retrieves a DeleteDesire.
func (s *Store) GetDeleteDesire(ctx context.Context, id desire.Identity) (desire.DeleteDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rk := id.ResourceKey()
	rec, exists := s.items[rk.String()]
	if !exists || !rec.Delete {
		return desire.DeleteDesire{}, desire.ErrNotFound
	}
	return s.projectDeleteDesire(rec), nil
}

// DeleteDeleteDesire deletes a DeleteDesire.
func (s *Store) DeleteDeleteDesire(ctx context.Context, id desire.Identity, owner string, version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rk := id.ResourceKey()
	key := rk.String()

	rec, exists := s.items[key]
	if !exists || !rec.Delete {
		return desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ErrVersionConflict
	}
	if err := desire.CheckOwner(ctx, rk, rec.Owner, owner); err != nil {
		return err
	}

	rec.Delete = false
	rec.DeleteResourceID = ""
	rec.DeleteStatus = desire.Status{}
	if rec.isEmpty() {
		delete(s.items, key)
	} else {
		rec.Version++
	}
	return nil
}

// CreateReadDesire creates a new ReadDesire.
func (s *Store) CreateReadDesire(ctx context.Context, d desire.ReadDesire) (desire.ReadDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.ReadDesire{}, fmt.Errorf("desire: validate read desire: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rk := d.Identity.ResourceKey()
	key := rk.String()

	rec, exists := s.items[key]
	if !exists {
		rec = &resourceRecord{
			Key:            rk,
			Owner:          d.Owner,
			Version:        1,
			Read:           true,
			ReadResourceID: d.ResourceID,
			ReadStatus:     desire.ReadStatus{},
		}
		s.items[key] = rec
		return s.projectReadDesire(rec), nil
	}

	if err := desire.CheckOwner(ctx, rk, rec.Owner, d.Owner); err != nil {
		return desire.ReadDesire{}, err
	}
	if rec.Read {
		return desire.ReadDesire{}, desire.ErrAlreadyExists
	}

	rec.Read = true
	rec.ReadResourceID = d.ResourceID
	rec.ReadStatus = desire.ReadStatus{}
	rec.Version++
	return s.projectReadDesire(rec), nil
}

// GetReadDesire retrieves a ReadDesire.
func (s *Store) GetReadDesire(ctx context.Context, id desire.Identity) (desire.ReadDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rk := id.ResourceKey()
	rec, exists := s.items[rk.String()]
	if !exists || !rec.Read {
		return desire.ReadDesire{}, desire.ErrNotFound
	}
	return s.projectReadDesire(rec), nil
}

// DeleteReadDesire deletes a ReadDesire.
func (s *Store) DeleteReadDesire(ctx context.Context, id desire.Identity, owner string, version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rk := id.ResourceKey()
	key := rk.String()

	rec, exists := s.items[key]
	if !exists || !rec.Read {
		return desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ErrVersionConflict
	}
	if err := desire.CheckOwner(ctx, rk, rec.Owner, owner); err != nil {
		return err
	}

	rec.Read = false
	rec.ReadResourceID = ""
	rec.ReadStatus = desire.ReadStatus{}
	if rec.isEmpty() {
		delete(s.items, key)
	} else {
		rec.Version++
	}
	return nil
}

// ListApplyDesires returns all ApplyDesires for a management cluster.
func (s *Store) ListApplyDesires(ctx context.Context, managementCluster string) ([]desire.ApplyDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]desire.ApplyDesire, 0, len(s.items))
	for _, rec := range s.items {
		if rec.Key.ManagementCluster == managementCluster && rec.Apply != nil {
			result = append(result, s.projectApplyDesire(rec))
		}
	}
	return result, nil
}

// ListDeleteDesires returns all DeleteDesires for a management cluster.
func (s *Store) ListDeleteDesires(ctx context.Context, managementCluster string) ([]desire.DeleteDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]desire.DeleteDesire, 0, len(s.items))
	for _, rec := range s.items {
		if rec.Key.ManagementCluster == managementCluster && rec.Delete {
			result = append(result, s.projectDeleteDesire(rec))
		}
	}
	return result, nil
}

// ListReadDesires returns all ReadDesires for a management cluster.
func (s *Store) ListReadDesires(ctx context.Context, managementCluster string) ([]desire.ReadDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]desire.ReadDesire, 0, len(s.items))
	for _, rec := range s.items {
		if rec.Key.ManagementCluster == managementCluster && rec.Read {
			result = append(result, s.projectReadDesire(rec))
		}
	}
	return result, nil
}

// DeleteByPrefix deletes desires matching a selector within a management cluster.
func (s *Store) DeleteByPrefix(ctx context.Context, managementCluster string, sel desire.PrefixSelector) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var toDelete []string
	for k, rec := range s.items {
		if rec.Key.ManagementCluster != managementCluster {
			continue
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

		if rec.isEmpty() {
			toDelete = append(toDelete, k)
			continue
		}
		if modified {
			rec.Version++
		}
	}

	for _, k := range toDelete {
		delete(s.items, k)
	}
	return nil
}

// UpdateApplyDesireStatus updates the status of an ApplyDesire.
func (s *Store) UpdateApplyDesireStatus(
	ctx context.Context, id desire.Identity, status desire.Status, version int64,
) (desire.ApplyDesire, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rk := id.ResourceKey()
	rec, exists := s.items[rk.String()]
	if !exists || rec.Apply == nil {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ApplyDesire{}, desire.ErrVersionConflict
	}

	rec.ApplyStatus = desire.CloneStatus(status)
	rec.Version++
	return s.projectApplyDesire(rec), nil
}

// UpdateDeleteDesireStatus updates the status of a DeleteDesire.
func (s *Store) UpdateDeleteDesireStatus(
	ctx context.Context, id desire.Identity, status desire.Status, version int64,
) (desire.DeleteDesire, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rk := id.ResourceKey()
	rec, exists := s.items[rk.String()]
	if !exists || !rec.Delete {
		return desire.DeleteDesire{}, desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.DeleteDesire{}, desire.ErrVersionConflict
	}

	rec.DeleteStatus = desire.CloneStatus(status)
	rec.Version++
	return s.projectDeleteDesire(rec), nil
}

// UpdateReadDesireStatus updates the status of a ReadDesire.
func (s *Store) UpdateReadDesireStatus(
	ctx context.Context, id desire.Identity, status desire.ReadStatus,
) (desire.ReadDesire, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rk := id.ResourceKey()
	rec, exists := s.items[rk.String()]
	if !exists || !rec.Read {
		return desire.ReadDesire{}, desire.ErrNotFound
	}

	rec.ReadStatus = desire.CloneReadStatus(status)
	return s.projectReadDesire(rec), nil
}

func (s *Store) projectApplyDesire(rec *resourceRecord) desire.ApplyDesire {
	if rec == nil || rec.Apply == nil {
		return desire.ApplyDesire{}
	}
	id := rec.Key.Identity(desire.TypeApply)
	return desire.ApplyDesire{
		Identity:   id,
		Owner:      rec.Owner,
		ResourceID: rec.ApplyResourceID,
		Version:    rec.Version,
		Spec:       desire.CloneApplySpec(*rec.Apply),
		Status:     desire.CloneStatus(rec.ApplyStatus),
	}
}

func (s *Store) projectDeleteDesire(rec *resourceRecord) desire.DeleteDesire {
	id := rec.Key.Identity(desire.TypeDelete)
	return desire.DeleteDesire{
		Identity:   id,
		Owner:      rec.Owner,
		ResourceID: rec.DeleteResourceID,
		Version:    rec.Version,
		Status:     desire.CloneStatus(rec.DeleteStatus),
	}
}

func (s *Store) projectReadDesire(rec *resourceRecord) desire.ReadDesire {
	id := rec.Key.Identity(desire.TypeRead)
	return desire.ReadDesire{
		Identity:   id,
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
