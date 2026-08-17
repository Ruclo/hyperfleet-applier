package desire_test

import (
	"strings"
	"testing"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

const (
	testOwner               = "test"
	fieldResource           = "resource"
	testManagementCluster   = "cluster-1"
	testGroup               = "apps"
	testResourceDeployments = "deployments"
	testNamespace           = "default"
	testName                = "my-app"
)

func validIdentity() desire.Identity {
	return desire.Identity{
		ManagementCluster: testManagementCluster,
		Type:              desire.TypeApply,
		Group:             testGroup,
		Resource:          testResourceDeployments,
		Namespace:         testNamespace,
		Name:              testName,
	}
}

func TestIdentity_Validate_RejectsEmptyRequiredFields(t *testing.T) {
	tests := []struct {
		zero  func(desire.Identity) desire.Identity
		name  string
		field string
	}{
		{
			name:  "managementCluster",
			field: "managementCluster",
			zero:  func(id desire.Identity) desire.Identity { id.ManagementCluster = ""; return id },
		},
		{
			name:  fieldResource,
			field: fieldResource,
			zero:  func(id desire.Identity) desire.Identity { id.Resource = ""; return id },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := validIdentity()
			id = tt.zero(id)

			if err := id.Validate(); err == nil {
				t.Errorf("expected validation to reject empty %q field", tt.field)
			}
		})
	}
}

func TestIdentity_Validate_AllowsEmptyOptionalFields(t *testing.T) {
	id := validIdentity()
	id.Group = ""
	id.Namespace = ""
	id.Name = ""

	if err := id.Validate(); err != nil {
		t.Errorf("expected validation to allow empty optional fields, got %v", err)
	}
}

func TestIdentity_Validate_RejectsTooLongFields(t *testing.T) {
	id := validIdentity()
	id.ManagementCluster = "x" + string(make([]byte, 253))

	if err := id.Validate(); err == nil {
		t.Errorf("expected validation to reject field exceeding RFC 1123 length limit")
	}
}

func TestIdentity_Validate_RejectsUppercase(t *testing.T) {
	id := validIdentity()
	id.ManagementCluster = "Cluster-1"

	if err := id.Validate(); err == nil {
		t.Errorf("expected validation to reject uppercase characters")
	}
}

func TestIdentity_Validate_RejectsColonAndSlash(t *testing.T) {
	tests := []struct {
		field func(*desire.Identity, string)
		name  string
	}{
		{name: "ManagementCluster", field: func(id *desire.Identity, v string) { id.ManagementCluster = v }},
		{name: "Resource", field: func(id *desire.Identity, v string) { id.Resource = v }},
		{name: "Namespace", field: func(id *desire.Identity, v string) { id.Namespace = v }},
		{name: "Name", field: func(id *desire.Identity, v string) { id.Name = v }},
	}

	for _, tt := range tests {
		t.Run(tt.name+":Colon", func(t *testing.T) {
			id := validIdentity()
			tt.field(&id, "value:invalid")

			if err := id.Validate(); err == nil {
				t.Errorf("expected validation to reject colon character")
			}
		})

		t.Run(tt.name+":Slash", func(t *testing.T) {
			id := validIdentity()
			tt.field(&id, "value/invalid")

			if err := id.Validate(); err == nil {
				t.Errorf("expected validation to reject slash character")
			}
		})
	}
}

func TestApplyDesireValidate_RejectsInvalidIdentity(t *testing.T) {
	d := desire.ApplyDesire{
		Identity: desire.Identity{ManagementCluster: ""},
		Owner:    testOwner,
		Spec:     desire.ApplySpec{KubeContent: []byte(`{}`)},
	}

	if err := d.Validate(); err == nil {
		t.Errorf("expected ApplyDesire.Validate to reject invalid Identity")
	}
}

func TestApplyDesireValidate_RejectsEmptyOwner(t *testing.T) {
	d := desire.ApplyDesire{
		Identity: validIdentity(),
		Owner:    "",
		Spec:     desire.ApplySpec{KubeContent: []byte(`{}`)},
	}

	if err := d.Validate(); err == nil {
		t.Errorf("expected ApplyDesire.Validate to reject empty Owner")
	}
}

func TestApplyDesireValidate_RejectsEmptyKubeContent(t *testing.T) {
	d := desire.ApplyDesire{
		Identity: validIdentity(),
		Owner:    testOwner,
		Spec:     desire.ApplySpec{KubeContent: []byte{}},
	}

	if err := d.Validate(); err == nil {
		t.Errorf("expected ApplyDesire.Validate to reject empty KubeContent")
	}
}

func TestApplySpecValidate_RejectsEmptyKubeContent(t *testing.T) {
	if err := (desire.ApplySpec{}).Validate(); err == nil {
		t.Errorf("expected ApplySpec.Validate to reject empty KubeContent")
	}
}

func TestApplyDesireValidate_RejectsWrongIdentityType(t *testing.T) {
	id := validIdentity()
	id.Type = desire.TypeDelete
	d := desire.ApplyDesire{
		Identity: id,
		Owner:    testOwner,
		Spec:     desire.ApplySpec{KubeContent: []byte(`{}`)},
	}

	if err := d.Validate(); err == nil {
		t.Errorf("expected ApplyDesire.Validate to reject Identity.Type %q", id.Type)
	}
}

func TestDeleteDesireValidate_RejectsInvalidIdentity(t *testing.T) {
	d := desire.DeleteDesire{
		Identity: desire.Identity{Type: desire.TypeDelete, ManagementCluster: ""},
		Owner:    testOwner,
	}

	if err := d.Validate(); err == nil {
		t.Errorf("expected DeleteDesire.Validate to reject invalid Identity")
	}
}

func TestDeleteDesireValidate_RejectsEmptyOwner(t *testing.T) {
	id := validIdentity()
	id.Type = desire.TypeDelete
	d := desire.DeleteDesire{
		Identity: id,
		Owner:    "",
	}

	if err := d.Validate(); err == nil {
		t.Errorf("expected DeleteDesire.Validate to reject empty Owner")
	}
}

func TestDeleteDesireValidate_RejectsWrongIdentityType(t *testing.T) {
	d := desire.DeleteDesire{
		Identity: validIdentity(),
		Owner:    testOwner,
	}

	if err := d.Validate(); err == nil {
		t.Errorf("expected DeleteDesire.Validate to reject Identity.Type %q", d.Identity.Type)
	}
}

func TestReadDesireValidate_RejectsInvalidIdentity(t *testing.T) {
	d := desire.ReadDesire{
		Identity: desire.Identity{Type: desire.TypeRead, ManagementCluster: ""},
		Owner:    testOwner,
	}

	if err := d.Validate(); err == nil {
		t.Errorf("expected ReadDesire.Validate to reject invalid Identity")
	}
}

func TestReadDesireValidate_RejectsEmptyOwner(t *testing.T) {
	id := validIdentity()
	id.Type = desire.TypeRead
	d := desire.ReadDesire{
		Identity: id,
		Owner:    "",
	}

	if err := d.Validate(); err == nil {
		t.Errorf("expected ReadDesire.Validate to reject empty Owner")
	}
}

func TestReadDesireValidate_RejectsWrongIdentityType(t *testing.T) {
	d := desire.ReadDesire{
		Identity: validIdentity(),
		Owner:    testOwner,
	}

	if err := d.Validate(); err == nil {
		t.Errorf("expected ReadDesire.Validate to reject Identity.Type %q", d.Identity.Type)
	}
}

func TestIdentity_Validate_RejectsInvalidDesireType(t *testing.T) {
	id := validIdentity()
	id.Type = "invalid"

	if err := id.Validate(); err == nil {
		t.Errorf("expected validation to reject invalid DesireType")
	}
}

func TestIdentity_Validate_RejectsDotsInLabelFields(t *testing.T) {
	tests := []struct {
		field func(*desire.Identity, string)
		name  string
	}{
		{name: "ManagementCluster", field: func(id *desire.Identity, v string) { id.ManagementCluster = v }},
		{name: "Resource", field: func(id *desire.Identity, v string) { id.Resource = v }},
		{name: "Namespace", field: func(id *desire.Identity, v string) { id.Namespace = v }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := validIdentity()
			tt.field(&id, "value.invalid")

			if err := id.Validate(); err == nil {
				t.Errorf("expected validation to reject dot in DNS label field %s", tt.name)
			}
		})
	}
}

func TestIdentity_Validate_AllowsDotsInNameField(t *testing.T) {
	id := validIdentity()
	id.Name = "kube-root-ca.crt"

	if err := id.Validate(); err != nil {
		t.Errorf("expected validation to allow DNS-1123 subdomain object name, got %v", err)
	}
}

func TestIdentity_Validate_AllowsDotsInGroupField(t *testing.T) {
	id := validIdentity()
	id.Group = "my.custom.group"

	if err := id.Validate(); err != nil {
		t.Errorf("expected validation to allow dots in DNS-1123 subdomain field group, got %v", err)
	}
}

func TestIdentity_Validate_RejectsInvalidGroup(t *testing.T) {
	tests := []struct {
		name  string
		group string
	}{
		{name: "tooLong", group: strings.Repeat("a", 254)},
		{name: "uppercase", group: "Apps"},
		{name: "colon", group: "bad:group"},
		{name: "leadingHyphen", group: "-apps"},
		{name: "trailingHyphen", group: "apps-"},
		{name: "leadingDot", group: ".apps"},
		{name: "trailingDot", group: "apps."},
		{name: "doubleDot", group: "apps..example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := validIdentity()
			id.Group = tt.group

			if err := id.Validate(); err == nil {
				t.Errorf("expected validation to reject group %q", tt.group)
			}
		})
	}
}
