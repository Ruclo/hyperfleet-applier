package desire

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// Validate checks that an Identity is valid.
// The identity tuple is:
//
//	managementCluster (DNS-1123 label, ≤63, required)
//	group (DNS-1123 subdomain, ≤253, optional)
//	resource (DNS-1035 label, ≤63, required)
//	namespace (DNS-1123 label, ≤63, optional, no dots)
//	name (DNS-1123 subdomain, ≤253, optional)
func (id Identity) Validate() error {
	if id.Type != TypeApply && id.Type != TypeDelete && id.Type != TypeRead {
		return fmt.Errorf("field type: value %q is not a valid DesireType (apply, delete, or read)", id.Type)
	}
	if err := validateDNS1123Label("managementCluster", id.ManagementCluster, true); err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	if err := validateDNS1123Subdomain("group", id.Group, false); err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	if err := validateDNS1035Label("resource", id.Resource, true); err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	if err := validateDNS1123Label("namespace", id.Namespace, false); err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	if err := validateDNS1123Subdomain("name", id.Name, false); err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	return nil
}

func validateDNS1035Label(field string, value string, required bool) error {
	if value == "" {
		if required {
			return fmt.Errorf("field %s is required and must be non-empty", field)
		}
		return nil
	}
	if errs := validation.IsDNS1035Label(value); len(errs) > 0 {
		return fmt.Errorf("field %s: value %q is not a valid DNS-1035 label: %s", field, value, strings.Join(errs, "; "))
	}
	return nil
}

func validateDNS1123Label(field string, value string, required bool) error {
	if value == "" {
		if required {
			return fmt.Errorf("field %s is required and must be non-empty", field)
		}
		return nil
	}
	if errs := validation.IsDNS1123Label(value); len(errs) > 0 {
		return fmt.Errorf("field %s: value %q is not a valid DNS-1123 label: %s", field, value, strings.Join(errs, "; "))
	}
	return nil
}

func validateDNS1123Subdomain(field string, value string, required bool) error {
	if value == "" {
		if required {
			return fmt.Errorf("field %s is required and must be non-empty", field)
		}
		return nil
	}
	if errs := validation.IsDNS1123Subdomain(value); len(errs) > 0 {
		return fmt.Errorf(
			"field %s: value %q is not a valid DNS-1123 subdomain: %s", field, value, strings.Join(errs, "; "),
		)
	}
	return nil
}

// Validate checks ApplySpec field consistency.
func (s ApplySpec) Validate() error {
	if len(s.KubeContent) == 0 {
		return errors.New("ApplySpec.KubeContent must not be empty")
	}
	if !json.Valid(s.KubeContent) {
		return errors.New("ApplySpec.KubeContent must be valid JSON")
	}
	return nil
}

// Validate checks ApplyDesire for field consistency.
func (d ApplyDesire) Validate() error {
	if d.Identity.Type != TypeApply {
		return fmt.Errorf("ApplyDesire: identity type must be %q, got %q", TypeApply, d.Identity.Type)
	}
	if err := d.Identity.Validate(); err != nil {
		return fmt.Errorf("ApplyDesire: %w", err)
	}
	if d.Owner == "" {
		return errors.New("ApplyDesire.Owner is required")
	}
	if err := d.Spec.Validate(); err != nil {
		return fmt.Errorf("ApplyDesire: %w", err)
	}
	return nil
}

// Validate checks DeleteDesire for field consistency.
func (d DeleteDesire) Validate() error {
	if d.Identity.Type != TypeDelete {
		return fmt.Errorf("DeleteDesire: identity type must be %q, got %q", TypeDelete, d.Identity.Type)
	}
	if err := d.Identity.Validate(); err != nil {
		return fmt.Errorf("DeleteDesire: %w", err)
	}
	if d.Owner == "" {
		return errors.New("DeleteDesire.Owner is required")
	}
	return nil
}

// Validate checks ReadDesire for field consistency.
func (d ReadDesire) Validate() error {
	if d.Identity.Type != TypeRead {
		return fmt.Errorf("ReadDesire: identity type must be %q, got %q", TypeRead, d.Identity.Type)
	}
	if err := d.Identity.Validate(); err != nil {
		return fmt.Errorf("ReadDesire: %w", err)
	}
	if d.Owner == "" {
		return errors.New("ReadDesire.Owner is required")
	}
	if d.TargetVersion == "" {
		return errors.New("ReadDesire.TargetVersion is required")
	}
	return nil
}
