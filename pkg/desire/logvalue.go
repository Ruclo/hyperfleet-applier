package desire

import "log/slog"

// LogValue implements slog.LogValuer so controllers can pass an Identity as a
// single slog attribute (e.g. "identity", id) instead of unwrapping fields by
// hand. Shared across apply/delete/read controllers.
func (id Identity) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("management_cluster", id.ManagementCluster),
		slog.String("type", string(id.Type)),
		slog.String("group", id.Group),
		slog.String("resource", id.Resource),
		slog.String("namespace", id.Namespace),
		slog.String("name", id.Name),
	)
}
