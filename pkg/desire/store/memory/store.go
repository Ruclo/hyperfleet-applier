// Package memory provides an in-memory desire store.
package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

// resourceRecord stores one desire keyed by full Identity.
// Apply uses Apply+Status, Delete uses Status, and Read uses ReadStatus.
type resourceRecord struct {
	Apply         *desire.ApplySpec
	Identity      desire.Identity
	OriginID      string
	Owner         string
	TargetVersion string
	ReadStatus    desire.ReadStatus
	Status        desire.Status
	Version       int64
}

// Store is a single-process in-memory implementation of SpecStore and StatusStore.
type Store struct {
	items map[desire.Identity]*resourceRecord
	mu    sync.RWMutex
}

// New creates a new in-memory store.
func New() *Store {
	return &Store{
		items: make(map[desire.Identity]*resourceRecord),
	}
}

// targetOwner returns the shared owner for a target across desire types.
func (s *Store) targetOwner(id desire.Identity) (string, bool) {
	for _, t := range desire.AllTypes() {
		sib := id
		sib.Type = t
		if rec, ok := s.items[sib]; ok {
			return rec.Owner, true
		}
	}
	return "", false
}

// checkTargetOwner enforces the single owner-per-target rule for a Create.
func (s *Store) checkTargetOwner(ctx context.Context, id desire.Identity, attempted string) error {
	if owner, ok := s.targetOwner(id); ok {
		return desire.CheckOwner(ctx, id, owner, attempted)
	}
	return nil
}

// CreateApplyDesire creates a new ApplyDesire or returns an error.
func (s *Store) CreateApplyDesire(ctx context.Context, d desire.ApplyDesire) (desire.ApplyDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.ApplyDesire{}, fmt.Errorf("desire: validate apply desire: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	id := d.Identity
	if err := s.checkTargetOwner(ctx, id, d.Owner); err != nil {
		return desire.ApplyDesire{}, fmt.Errorf("desire: create apply desire %s: %w", id, err)
	}

	deleteID := id
	deleteID.Type = desire.TypeDelete
	if deleteRecord, ok := s.items[deleteID]; ok {
		if !desire.IsDeleted(deleteRecord.Status) {
			return desire.ApplyDesire{}, desire.ErrDeletePending
		}
		// Retire the completed delete with the new apply.
		delete(s.items, deleteID)
	}
	if _, ok := s.items[id]; ok {
		return desire.ApplyDesire{}, desire.ErrAlreadyExists
	}

	spec := desire.CloneApplySpec(d.Spec)
	rec := &resourceRecord{
		Identity: id,
		Owner:    d.Owner,
		OriginID: d.OriginID,
		Version:  1,
		Apply:    &spec,
	}
	s.items[id] = rec
	return s.projectApplyDesire(rec), nil
}

// GetApplyDesire retrieves an ApplyDesire or returns ErrNotFound.
func (s *Store) GetApplyDesire(ctx context.Context, id desire.Identity) (desire.ApplyDesire, error) {
	if id.Type != desire.TypeApply {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, exists := s.items[id]
	if !exists || rec.Apply == nil {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}
	return s.projectApplyDesire(rec), nil
}

// UpdateApplyDesireSpec updates the spec and owner of an ApplyDesire.
func (s *Store) UpdateApplyDesireSpec(
	ctx context.Context, id desire.Identity, spec desire.ApplySpec, owner string, version int64,
) (desire.ApplyDesire, error) {
	if id.Type != desire.TypeApply {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}
	if err := spec.Validate(); err != nil {
		return desire.ApplyDesire{}, fmt.Errorf("desire: validate apply spec: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.items[id]
	if !exists || rec.Apply == nil {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ApplyDesire{}, desire.ErrVersionConflict
	}
	if err := desire.CheckOwner(ctx, id, rec.Owner, owner); err != nil {
		return desire.ApplyDesire{}, fmt.Errorf("desire: update apply desire spec %s: %w", id, err)
	}

	cloned := desire.CloneApplySpec(spec)
	rec.Apply = &cloned
	// Clear status because it described the old spec.
	rec.Status = desire.Status{}
	rec.Version++
	return s.projectApplyDesire(rec), nil
}

// DeleteApplyDesire deletes an ApplyDesire.
func (s *Store) DeleteApplyDesire(ctx context.Context, id desire.Identity, owner string, version int64) error {
	if id.Type != desire.TypeApply {
		return desire.ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.items[id]
	if !exists || rec.Apply == nil {
		return desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ErrVersionConflict
	}
	if err := desire.CheckOwner(ctx, id, rec.Owner, owner); err != nil {
		return fmt.Errorf("desire: delete apply desire %s: %w", id, err)
	}

	delete(s.items, id)
	return nil
}

// CreateDeleteDesire creates a new DeleteDesire.
func (s *Store) CreateDeleteDesire(ctx context.Context, d desire.DeleteDesire) (desire.DeleteDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.DeleteDesire{}, fmt.Errorf("desire: validate delete desire: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	id := d.Identity
	if err := s.checkTargetOwner(ctx, id, d.Owner); err != nil {
		return desire.DeleteDesire{}, fmt.Errorf("desire: create delete desire %s: %w", id, err)
	}
	if _, ok := s.items[id]; ok {
		return desire.DeleteDesire{}, desire.ErrAlreadyExists
	}

	// Delete supersedes an existing Apply for the same target.
	applyID := id
	applyID.Type = desire.TypeApply
	delete(s.items, applyID)

	rec := &resourceRecord{
		Identity: id,
		Owner:    d.Owner,
		OriginID: d.OriginID,
		Version:  1,
	}
	s.items[id] = rec
	return s.projectDeleteDesire(rec), nil
}

// GetDeleteDesire retrieves a DeleteDesire.
func (s *Store) GetDeleteDesire(ctx context.Context, id desire.Identity) (desire.DeleteDesire, error) {
	if id.Type != desire.TypeDelete {
		return desire.DeleteDesire{}, desire.ErrNotFound
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, exists := s.items[id]
	if !exists {
		return desire.DeleteDesire{}, desire.ErrNotFound
	}
	return s.projectDeleteDesire(rec), nil
}

// DeleteDeleteDesire deletes a DeleteDesire.
func (s *Store) DeleteDeleteDesire(ctx context.Context, id desire.Identity, owner string, version int64) error {
	if id.Type != desire.TypeDelete {
		return desire.ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.items[id]
	if !exists {
		return desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ErrVersionConflict
	}
	if err := desire.CheckOwner(ctx, id, rec.Owner, owner); err != nil {
		return fmt.Errorf("desire: delete delete desire %s: %w", id, err)
	}

	delete(s.items, id)
	return nil
}

// CreateReadDesire creates a new ReadDesire.
func (s *Store) CreateReadDesire(ctx context.Context, d desire.ReadDesire) (desire.ReadDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.ReadDesire{}, fmt.Errorf("desire: validate read desire: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	id := d.Identity
	if err := s.checkTargetOwner(ctx, id, d.Owner); err != nil {
		return desire.ReadDesire{}, fmt.Errorf("desire: create read desire %s: %w", id, err)
	}
	if _, ok := s.items[id]; ok {
		return desire.ReadDesire{}, desire.ErrAlreadyExists
	}

	rec := &resourceRecord{
		Identity:      id,
		Owner:         d.Owner,
		OriginID:      d.OriginID,
		TargetVersion: d.TargetVersion,
		Version:       1,
	}
	s.items[id] = rec
	return s.projectReadDesire(rec), nil
}

// GetReadDesire retrieves a ReadDesire.
func (s *Store) GetReadDesire(ctx context.Context, id desire.Identity) (desire.ReadDesire, error) {
	if id.Type != desire.TypeRead {
		return desire.ReadDesire{}, desire.ErrNotFound
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, exists := s.items[id]
	if !exists {
		return desire.ReadDesire{}, desire.ErrNotFound
	}
	return s.projectReadDesire(rec), nil
}

// DeleteReadDesire deletes a ReadDesire.
func (s *Store) DeleteReadDesire(ctx context.Context, id desire.Identity, owner string, version int64) error {
	if id.Type != desire.TypeRead {
		return desire.ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.items[id]
	if !exists {
		return desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ErrVersionConflict
	}
	if err := desire.CheckOwner(ctx, id, rec.Owner, owner); err != nil {
		return fmt.Errorf("desire: delete read desire %s: %w", id, err)
	}

	delete(s.items, id)
	return nil
}

// ListApplyDesires returns all ApplyDesires for a management cluster.
func (s *Store) ListApplyDesires(ctx context.Context, managementCluster string) ([]desire.ApplyDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]desire.ApplyDesire, 0, len(s.items))
	for id, rec := range s.items {
		if id.ManagementCluster == managementCluster && id.Type == desire.TypeApply {
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
	for id, rec := range s.items {
		if id.ManagementCluster == managementCluster && id.Type == desire.TypeDelete {
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
	for id, rec := range s.items {
		if id.ManagementCluster == managementCluster && id.Type == desire.TypeRead {
			result = append(result, s.projectReadDesire(rec))
		}
	}
	return result, nil
}

// DeleteByPrefix deletes desires matching a selector within a management cluster.
func (s *Store) DeleteByPrefix(ctx context.Context, managementCluster string, sel desire.PrefixSelector) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id := range s.items {
		if id.ManagementCluster != managementCluster {
			continue
		}
		if sel.Matches(id) {
			delete(s.items, id)
		}
	}
	return nil
}

// UpdateApplyDesireStatus updates the status of an ApplyDesire.
func (s *Store) UpdateApplyDesireStatus(
	ctx context.Context, id desire.Identity, status desire.Status, version int64,
) (desire.ApplyDesire, error) {
	if id.Type != desire.TypeApply {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.items[id]
	if !exists || rec.Apply == nil {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ApplyDesire{}, desire.ErrVersionConflict
	}

	rec.Status = desire.CloneStatus(status)
	rec.Version++
	return s.projectApplyDesire(rec), nil
}

// UpdateDeleteDesireStatus updates the status of a DeleteDesire.
func (s *Store) UpdateDeleteDesireStatus(
	ctx context.Context, id desire.Identity, status desire.Status, version int64,
) (desire.DeleteDesire, error) {
	if id.Type != desire.TypeDelete {
		return desire.DeleteDesire{}, desire.ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.items[id]
	if !exists {
		return desire.DeleteDesire{}, desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.DeleteDesire{}, desire.ErrVersionConflict
	}

	rec.Status = desire.CloneStatus(status)
	rec.Version++
	return s.projectDeleteDesire(rec), nil
}

// UpdateReadDesireStatus updates the status of a ReadDesire.
func (s *Store) UpdateReadDesireStatus(
	ctx context.Context, id desire.Identity, status desire.ReadStatus,
) (desire.ReadDesire, error) {
	if id.Type != desire.TypeRead {
		return desire.ReadDesire{}, desire.ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.items[id]
	if !exists {
		return desire.ReadDesire{}, desire.ErrNotFound
	}

	rec.ReadStatus = desire.CloneReadStatus(status)
	return s.projectReadDesire(rec), nil
}

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
