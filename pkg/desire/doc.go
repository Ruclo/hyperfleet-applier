// Package desire defines the core desire types for the desire-based delivery system.
//
// The desire model replaces Maestro/OCM ManifestWork as the transport mechanism
// for delivering Kubernetes resources to target clusters. Three desire types exist:
//
//   - ApplyDesire: make a resource exist with specific content (SSA force=true)
//   - DeleteDesire: make a resource not exist (confirmed gone past finalizers)
//   - ReadDesire: mirror a live object's state back to the control plane
//
// Each desire targets one Kubernetes resource (Identity). The store also
// supports listing and prefix delete (ListApplyDesires, DeleteByPrefix).
//
// Behavioral semantics are aligned with the ARO-HCP kube-applier specification.
//
// Store interfaces (SpecStore, StatusStore) live here; backends live in
// store/memory and store/redis.
package desire
