package auth

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type Role string

const (
	RoleAdmin    Role = "admin"
	RoleManager  Role = "manager"
	RoleOperator Role = "operator"
	RoleViewer   Role = "viewer"
)

type Resource string

const (
	ResourcePipeline  Resource = "pipeline"
	ResourceJob       Resource = "job"
	ResourceWorker    Resource = "worker"
	ResourceSecret    Resource = "secret"
	ResourceConfig    Resource = "config"
	ResourceUser      Resource = "user"
	ResourceAuditLog  Resource = "audit_log"
	ResourceNamespace Resource = "namespace"
	ResourceAll       Resource = "*"
)

type Action string

const (
	ActionCreate Action = "create"
	ActionRead   Action = "read"
	ActionUpdate Action = "update"
	ActionDelete Action = "delete"
	ActionAll    Action = "*"
)

type PermissionSet map[string]map[string]bool

type RoleDefinition struct {
	Role        Role
	Level       int
	Permissions PermissionSet
	Inherits    []Role
}

type RBACEnforcer struct {
	mu          sync.RWMutex
	roles       map[Role]*RoleDefinition
	customRoles map[string]*RoleDefinition
}

func NewRBACEnforcer() *RBACEnforcer {
	e := &RBACEnforcer{
		roles:       make(map[Role]*RoleDefinition),
		customRoles: make(map[string]*RoleDefinition),
	}
	e.loadDefaultRoles()
	return e
}

func (e *RBACEnforcer) loadDefaultRoles() {
	e.roles[RoleViewer] = &RoleDefinition{
		Role:  RoleViewer,
		Level: 0,
		Permissions: PermissionSet{
			string(ResourcePipeline): {string(ActionRead): true},
			string(ResourceJob):      {string(ActionRead): true},
			string(ResourceAuditLog): {string(ActionRead): true},
		},
	}

	e.roles[RoleOperator] = &RoleDefinition{
		Role:  RoleOperator,
		Level: 1,
		Permissions: PermissionSet{
			string(ResourcePipeline): {string(ActionCreate): true, string(ActionRead): true, string(ActionUpdate): true},
			string(ResourceJob):      {string(ActionCreate): true, string(ActionRead): true, string(ActionUpdate): true, string(ActionDelete): true},
			string(ResourceWorker):   {string(ActionRead): true, string(ActionUpdate): true},
			string(ResourceAuditLog): {string(ActionRead): true},
		},
		Inherits: []Role{RoleViewer},
	}

	e.roles[RoleManager] = &RoleDefinition{
		Role:  RoleManager,
		Level: 2,
		Permissions: PermissionSet{
			string(ResourcePipeline):  {string(ActionAll): true},
			string(ResourceJob):       {string(ActionAll): true},
			string(ResourceWorker):    {string(ActionAll): true},
			string(ResourceSecret):    {string(ActionCreate): true, string(ActionRead): true, string(ActionUpdate): true},
			string(ResourceConfig):    {string(ActionCreate): true, string(ActionRead): true, string(ActionUpdate): true},
			string(ResourceAuditLog):  {string(ActionRead): true},
			string(ResourceNamespace): {string(ActionRead): true, string(ActionUpdate): true},
		},
		Inherits: []Role{RoleOperator},
	}

	e.roles[RoleAdmin] = &RoleDefinition{
		Role:  RoleAdmin,
		Level: 3,
		Permissions: PermissionSet{
			string(ResourceAll): {string(ActionAll): true},
		},
		Inherits: []Role{RoleManager},
	}
}

func (e *RBACEnforcer) AddCustomRole(name string, level int, permissions PermissionSet, inherits []Role) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.roles[Role(name)]; exists {
		return fmt.Errorf("role %q already exists as a built-in role", name)
	}
	if _, exists := e.customRoles[name]; exists {
		return fmt.Errorf("custom role %q already exists", name)
	}

	role := &RoleDefinition{
		Role:        Role(name),
		Level:       level,
		Permissions: permissions,
		Inherits:    inherits,
	}
	e.customRoles[name] = role
	return nil
}

func (e *RBACEnforcer) CheckPermission(role Role, resource Resource, action Action) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	roleDef, exists := e.roles[role]
	if !exists {
		roleDef, exists = e.customRoles[string(role)]
		if !exists {
			return false
		}
	}

	return e.hasPermission(roleDef, string(resource), string(action))
}

func (e *RBACEnforcer) CheckAnyPermission(role Role, resource Resource, actions ...Action) bool {
	for _, action := range actions {
		if e.CheckPermission(role, resource, action) {
			return true
		}
	}
	return false
}

func (e *RBACEnforcer) CheckAllPermissions(role Role, resource Resource, actions ...Action) bool {
	for _, action := range actions {
		if !e.CheckPermission(role, resource, action) {
			return false
		}
	}
	return true
}

func (e *RBACEnforcer) HasRole(userRoles []Role, target Role) bool {
	for _, r := range userRoles {
		if r == target {
			return true
		}
	}
	return false
}

func (e *RBACEnforcer) GetEffectivePermissions(roles []Role) PermissionSet {
	merged := make(PermissionSet)

	for _, role := range roles {
		roleDef, exists := e.roles[role]
		if !exists {
			roleDef, exists = e.customRoles[string(role)]
			if !exists {
				continue
			}
		}
		e.mergePermissions(merged, roleDef)
	}

	return merged
}

func (e *RBACEnforcer) RequiresPermission(resource Resource, action Action) bool {
	return true
}

func (e *RBACEnforcer) ListRoles() []Role {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var roles []Role
	for role := range e.roles {
		roles = append(roles, role)
	}
	for _, def := range e.customRoles {
		roles = append(roles, def.Role)
	}

	sort.Slice(roles, func(i, j int) bool {
		return e.getRoleLevel(roles[i]) < e.getRoleLevel(roles[j])
	})
	return roles
}

func (e *RBACEnforcer) getRoleLevel(role Role) int {
	if def, ok := e.roles[role]; ok {
		return def.Level
	}
	if def, ok := e.customRoles[string(role)]; ok {
		return def.Level
	}
	return -1
}

func (e *RBACEnforcer) hasPermission(roleDef *RoleDefinition, resource, action string) bool {
	if perms, ok := roleDef.Permissions[resource]; ok {
		if perms[action] || perms[string(ActionAll)] {
			return true
		}
	}
	if perms, ok := roleDef.Permissions[string(ResourceAll)]; ok {
		if perms[action] || perms[string(ActionAll)] {
			return true
		}
	}

	for _, inherited := range roleDef.Inherits {
		if inheritedDef, ok := e.roles[inherited]; ok {
			if e.hasPermission(inheritedDef, resource, action) {
				return true
			}
		}
	}

	return false
}

func (e *RBACEnforcer) mergePermissions(merged PermissionSet, roleDef *RoleDefinition) {
	for resource, actions := range roleDef.Permissions {
		if _, ok := merged[resource]; !ok {
			merged[resource] = make(map[string]bool)
		}
		for action := range actions {
			merged[resource][action] = true
		}
	}

	for _, inherited := range roleDef.Inherits {
		if inheritedDef, ok := e.roles[inherited]; ok {
			e.mergePermissions(merged, inheritedDef)
		}
	}
}

func (e *RBACEnforcer) ValidateRoleTransition(currentRoles []Role, targetRole Role) error {
	currentLevel := 0
	for _, r := range currentRoles {
		if l := e.getRoleLevel(r); l > currentLevel {
			currentLevel = l
		}
	}

	targetLevel := e.getRoleLevel(targetRole)
	if targetLevel < 0 {
		return fmt.Errorf("unknown role: %s", targetRole)
	}

	if targetLevel > currentLevel+1 && currentLevel > 0 {
		return fmt.Errorf("cannot promote from level %d to level %d in one step", currentLevel, targetLevel)
	}

	return nil
}

func PermissionFor(resource Resource, action Action) string {
	return fmt.Sprintf("%s:%s", resource, action)
}

func ParsePermission(perm string) (Resource, Action, error) {
	parts := strings.SplitN(perm, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid permission format: %s", perm)
	}
	return Resource(parts[0]), Action(parts[1]), nil
}
