package auth

import (
	"testing"
)

func TestNewRBACEnforcer(t *testing.T) {
	e := NewRBACEnforcer()
	if e == nil {
		t.Fatal("expected non-nil RBACEnforcer")
	}

	roles := e.ListRoles()
	if len(roles) == 0 {
		t.Fatal("expected at least one default role")
	}
}

func TestRoleHierarchy(t *testing.T) {
	e := NewRBACEnforcer()

	tests := []struct {
		role     Role
		resource Resource
		action   Action
		allowed  bool
	}{
		{RoleViewer, ResourcePipeline, ActionRead, true},
		{RoleViewer, ResourcePipeline, ActionCreate, false},
		{RoleViewer, ResourcePipeline, ActionDelete, false},
		{RoleViewer, ResourceJob, ActionRead, true},
		{RoleViewer, ResourceWorker, ActionRead, false},
		{RoleViewer, ResourceSecret, ActionRead, false},

		{RoleOperator, ResourcePipeline, ActionRead, true},
		{RoleOperator, ResourcePipeline, ActionCreate, true},
		{RoleOperator, ResourcePipeline, ActionUpdate, true},
		{RoleOperator, ResourcePipeline, ActionDelete, false},
		{RoleOperator, ResourceJob, ActionRead, true},
		{RoleOperator, ResourceJob, ActionCreate, true},
		{RoleOperator, ResourceJob, ActionDelete, true},
		{RoleOperator, ResourceWorker, ActionRead, true},
		{RoleOperator, ResourceWorker, ActionUpdate, true},
		{RoleOperator, ResourceSecret, ActionRead, false},
		{RoleOperator, ResourceAuditLog, ActionRead, true},

		{RoleManager, ResourcePipeline, ActionAll, true},
		{RoleManager, ResourceJob, ActionDelete, true},
		{RoleManager, ResourceWorker, ActionDelete, true},
		{RoleManager, ResourceSecret, ActionRead, true},
		{RoleManager, ResourceSecret, ActionCreate, true},
		{RoleManager, ResourceSecret, ActionDelete, false},
		{RoleManager, ResourceConfig, ActionRead, true},
		{RoleManager, ResourceConfig, ActionCreate, true},
		{RoleManager, ResourceConfig, ActionDelete, false},
		{RoleManager, ResourceUser, ActionRead, false},
		{RoleManager, ResourceNamespace, ActionRead, true},
	}

	for _, tc := range tests {
		got := e.CheckPermission(tc.role, tc.resource, tc.action)
		if got != tc.allowed {
			t.Errorf("CheckPermission(%q, %q, %q) = %v, want %v",
				tc.role, tc.resource, tc.action, got, tc.allowed)
		}
	}
}

func TestAdminHasAllPermissions(t *testing.T) {
	e := NewRBACEnforcer()

	resources := []Resource{
		ResourcePipeline, ResourceJob, ResourceWorker,
		ResourceSecret, ResourceConfig, ResourceUser,
		ResourceAuditLog, ResourceNamespace,
	}
	actions := []Action{ActionCreate, ActionRead, ActionUpdate, ActionDelete}

	for _, res := range resources {
		for _, act := range actions {
			if !e.CheckPermission(RoleAdmin, res, act) {
				t.Errorf("admin should have %s:%s", res, act)
			}
		}
	}
}

func TestAdminAllWildcard(t *testing.T) {
	e := NewRBACEnforcer()

	if !e.CheckPermission(RoleAdmin, ResourceAll, ActionAll) {
		t.Error("admin should have *:*")
	}
	if !e.CheckPermission(RoleAdmin, ResourcePipeline, ActionAll) {
		t.Error("admin should have all actions on pipeline")
	}
}

func TestCheckAnyPermission(t *testing.T) {
	e := NewRBACEnforcer()

	ok := e.CheckAnyPermission(RoleViewer, ResourcePipeline, ActionCreate, ActionRead, ActionDelete)
	if !ok {
		t.Error("viewer should have at least one of these on pipeline")
	}

	ok = e.CheckAnyPermission(RoleViewer, ResourceSecret, ActionCreate, ActionDelete, ActionUpdate)
	if ok {
		t.Error("viewer should have none of these on secret")
	}
}

func TestCheckAllPermissions(t *testing.T) {
	e := NewRBACEnforcer()

	ok := e.CheckAllPermissions(RoleManager, ResourcePipeline, ActionCreate, ActionRead, ActionUpdate, ActionDelete)
	if !ok {
		t.Error("manager should have all actions on pipeline")
	}

	ok = e.CheckAllPermissions(RoleManager, ResourceSecret, ActionCreate, ActionRead)
	if !ok {
		t.Error("manager should have create+read on secret")
	}

	ok = e.CheckAllPermissions(RoleManager, ResourceSecret, ActionCreate, ActionRead, ActionDelete)
	if ok {
		t.Error("manager should NOT have delete on secret")
	}
}

func TestUnknownRole(t *testing.T) {
	e := NewRBACEnforcer()

	if e.CheckPermission("nonexistent", ResourcePipeline, ActionRead) {
		t.Error("unknown role should not have any permissions")
	}
}

func TestHasRole(t *testing.T) {
	e := NewRBACEnforcer()

	if !e.HasRole([]Role{RoleAdmin, RoleViewer}, RoleAdmin) {
		t.Error("expected admin role to be found")
	}
	if e.HasRole([]Role{RoleViewer}, RoleAdmin) {
		t.Error("expected admin role to not be found in viewer list")
	}
	if e.HasRole(nil, RoleAdmin) {
		t.Error("expected nil roles to not contain admin")
	}
}

func TestGetEffectivePermissions(t *testing.T) {
	e := NewRBACEnforcer()

	perms := e.GetEffectivePermissions([]Role{RoleViewer})
	if perms == nil {
		t.Fatal("expected non-nil permissions")
	}

	pipelinePerms, ok := perms[string(ResourcePipeline)]
	if !ok {
		t.Fatal("expected pipeline resource in viewer permissions")
	}
	if !pipelinePerms[string(ActionRead)] {
		t.Error("viewer should have read on pipeline")
	}
	if pipelinePerms[string(ActionCreate)] {
		t.Error("viewer should NOT have create on pipeline")
	}
}

func TestGetEffectivePermissionsAdmin(t *testing.T) {
	e := NewRBACEnforcer()

	perms := e.GetEffectivePermissions([]Role{RoleAdmin})
	if perms == nil {
		t.Fatal("expected non-nil permissions")
	}

	allPerms, ok := perms[string(ResourceAll)]
	if !ok {
		t.Fatal("expected ResourceAll in admin permissions")
	}
	if !allPerms[string(ActionAll)] {
		t.Fatal("admin should have ActionAll on ResourceAll")
	}
}

func TestListRolesOrdered(t *testing.T) {
	e := NewRBACEnforcer()
	roles := e.ListRoles()

	if len(roles) < 4 {
		t.Fatalf("expected at least 4 roles, got %d", len(roles))
	}
	expected := []Role{RoleViewer, RoleOperator, RoleManager, RoleAdmin}
	for i, exp := range expected {
		if roles[i] != exp {
			t.Errorf("index %d: expected %q, got %q", i, exp, roles[i])
		}
	}
}

func TestAddCustomRole(t *testing.T) {
	e := NewRBACEnforcer()

	err := e.AddCustomRole("deployer", 2, PermissionSet{
		string(ResourcePipeline): {string(ActionRead): true, string(ActionUpdate): true},
	}, []Role{RoleViewer})
	if err != nil {
		t.Fatalf("unexpected error adding custom role: %v", err)
	}

	if !e.CheckPermission("deployer", ResourcePipeline, ActionRead) {
		t.Error("deployer should have read on pipeline")
	}
	if !e.CheckPermission("deployer", ResourcePipeline, ActionUpdate) {
		t.Error("deployer should have update on pipeline")
	}
	if e.CheckPermission("deployer", ResourcePipeline, ActionDelete) {
		t.Error("deployer should NOT have delete on pipeline")
	}

	if !e.CheckPermission("deployer", ResourceJob, ActionRead) {
		t.Error("deployer should inherit read on job from viewer")
	}
}

func TestAddCustomRoleDuplicate(t *testing.T) {
	e := NewRBACEnforcer()
	err := e.AddCustomRole("deployer", 2, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = e.AddCustomRole("deployer", 3, nil, nil)
	if err == nil {
		t.Fatal("expected error adding duplicate custom role")
	}
}

func TestAddCustomRoleOverridesBuiltin(t *testing.T) {
	e := NewRBACEnforcer()
	err := e.AddCustomRole("admin", 5, nil, nil)
	if err == nil {
		t.Fatal("expected error adding custom role with built-in name")
	}
}

func TestValidateRoleTransition(t *testing.T) {
	e := NewRBACEnforcer()

	if err := e.ValidateRoleTransition([]Role{RoleViewer}, RoleOperator); err != nil {
		t.Errorf("viewer->operator should be valid: %v", err)
	}
	if err := e.ValidateRoleTransition([]Role{RoleOperator}, RoleManager); err != nil {
		t.Errorf("operator->manager should be valid: %v", err)
	}
	if err := e.ValidateRoleTransition([]Role{RoleManager}, RoleAdmin); err != nil {
		t.Errorf("manager->admin should be valid: %v", err)
	}
	if err := e.ValidateRoleTransition([]Role{RoleViewer}, RoleAdmin); err != nil {
		t.Logf("viewer->admin may be rejected: %v", err)
	}
	if err := e.ValidateRoleTransition([]Role{RoleViewer}, "nonexistent"); err == nil {
		t.Error("expected error for unknown target role")
	}
}

func TestPermissionFor(t *testing.T) {
	perm := PermissionFor(ResourcePipeline, ActionRead)
	if perm != "pipeline:read" {
		t.Errorf("expected 'pipeline:read', got %q", perm)
	}
}

func TestParsePermission(t *testing.T) {
	resource, action, err := ParsePermission("pipeline:create")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resource != ResourcePipeline {
		t.Errorf("expected ResourcePipeline, got %q", resource)
	}
	if action != ActionCreate {
		t.Errorf("expected ActionCreate, got %q", action)
	}
}

func TestParsePermissionInvalid(t *testing.T) {
	_, _, err := ParsePermission("invalid")
	if err == nil {
		t.Fatal("expected error for invalid permission format")
	}

	_, _, err = ParsePermission("too:many:parts")
	if err == nil {
		t.Fatal("expected error for too many colons")
	}
}
