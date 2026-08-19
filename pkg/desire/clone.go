package desire

import "slices"

// CloneApplySpec returns a deep copy of spec whose mutable fields share no
// backing storage with the input, so callers and stores cannot mutate each
// other's copy. A nil KubeContent stays nil.
func CloneApplySpec(spec ApplySpec) ApplySpec {
	spec.KubeContent = slices.Clone(spec.KubeContent)
	return spec
}

// CloneStatus returns a deep copy of status with an independent Conditions
// slice. A nil Conditions stays nil.
func CloneStatus(status Status) Status {
	status.Conditions = slices.Clone(status.Conditions)
	return status
}

// CloneReadStatus returns a deep copy of status with independent Conditions and
// KubeContent. Both stay nil when nil.
func CloneReadStatus(status ReadStatus) ReadStatus {
	status.Status = CloneStatus(status.Status)
	status.KubeContent = slices.Clone(status.KubeContent)
	return status
}
