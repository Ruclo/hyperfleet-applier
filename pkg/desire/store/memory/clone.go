package memory

import (
	"slices"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

func cloneApplySpec(in desire.ApplySpec) desire.ApplySpec {
	in.KubeContent = slices.Clone(in.KubeContent)
	return in
}

func cloneStatus(in desire.Status) desire.Status {
	in.Conditions = slices.Clone(in.Conditions)
	return in
}

func cloneReadStatus(in desire.ReadStatus) desire.ReadStatus {
	in.Status = cloneStatus(in.Status)
	in.KubeContent = slices.Clone(in.KubeContent)
	return in
}
