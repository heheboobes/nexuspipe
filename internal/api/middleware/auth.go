package middleware

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/nexuspipe/nexuspipe/internal/auth"
)

const (
	ContextKeyUserID      = "user_id"
	ContextKeyUsername    = "username"
	ContextKeyEmail       = "email"
	ContextKeyRoles       = "roles"
	ContextKeyPermissions = "permissions"
	ContextKeyAuthMethod  = "auth_method"
	ContextKeyClaims      = "claims"
	ContextKeyAPIKey      = "api_key"
)

type AuthConfig struct {
	JWTManager    *auth.JWTManager
	APIKeyManager *auth.APIKeyManager
	RBACEnforcer  *auth.RBACEnforcer
	SkipPaths     []string
}

func AuthMiddleware(config *AuthConfig) gin.HandlerFunc {
	skipMap := make(map[string]bool, len(config.SkipPaths))
	for _, p := range config.SkipPaths {
		skipMap[p] = true
	}

	return func(c *gin.Context) {
		if skipMap[c.Request.URL.Path] {
			c.Next()
			return
		}

		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authorization header is required"})
			return
		}

		if strings.HasPrefix(authHeader, "Bearer ") {
			if config.JWTManager != nil {
				token, err := auth.ExtractBearerToken(authHeader)
				if err != nil {
					c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
					return
				}
				claims, err := config.JWTManager.ValidateToken(token)
				if err != nil {
					status := http.StatusUnauthorized
					if errors.Is(err, auth.ErrTokenExpired) {
						status = http.StatusUnauthorized
					}
					c.AbortWithStatusJSON(status, gin.H{"error": err.Error()})
					return
				}
				setUserContext(c, claims.UserID, claims.Username, claims.Email, claims.Roles, claims.Permissions, "jwt")
				c.Set(ContextKeyClaims, claims)
				c.Next()
				return
			}
		} else if strings.HasPrefix(authHeader, "Basic ") {
			if err := basicAuthHandler(c, authHeader); err != nil {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
				return
			}
			c.Next()
			return
		} else {
			if config.APIKeyManager != nil {
				apiKey, err := config.APIKeyManager.ValidateKey(authHeader)
				if err != nil {
					c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid API key"})
					return
				}
				role := auth.RoleViewer
				if len(apiKey.Permissions) > 0 {
					role = auth.Role(strings.ToLower(apiKey.Permissions[0]))
				}
				setUserContext(c, apiKey.UserID, apiKey.Name, "", []string{string(role)}, nil, "apikey")
				c.Set(ContextKeyAPIKey, apiKey)
				c.Next()
				return
			}
		}

		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unsupported authorization method"})
	}
}

func JWTAuthMiddleware(jwtManager *auth.JWTManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authorization header is required"})
			return
		}

		token, err := auth.ExtractBearerToken(authHeader)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}

		claims, err := jwtManager.ValidateToken(token)
		if err != nil {
			status := http.StatusUnauthorized
			if errors.Is(err, auth.ErrTokenExpired) {
				status = http.StatusUnauthorized
			}
			c.AbortWithStatusJSON(status, gin.H{"error": err.Error()})
			return
		}

		setUserContext(c, claims.UserID, claims.Username, claims.Email, claims.Roles, claims.Permissions, "jwt")
		c.Set(ContextKeyClaims, claims)
		c.Next()
	}
}

func APIKeyAuthMiddleware(apiKeyManager *auth.APIKeyManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		apiKey := c.GetHeader("X-API-Key")
		if apiKey == "" {
			apiKey = c.Query("api_key")
		}
		if apiKey == "" {
			authHeader := c.GetHeader("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ") || strings.HasPrefix(authHeader, "Basic ") {
				c.Next()
				return
			}
			apiKey = authHeader
		}
		if apiKey == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "API key is required"})
			return
		}

		if err := apiKeyManager.CheckRateLimit(apiKey); err != nil {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}

		key, err := apiKeyManager.ValidateKey(apiKey)
		if err != nil {
			status := http.StatusUnauthorized
			if errors.Is(err, auth.ErrAPIKeyExpired) {
				status = http.StatusUnauthorized
			}
			c.AbortWithStatusJSON(status, gin.H{"error": err.Error()})
			return
		}

		role := auth.RoleViewer
		if len(key.Permissions) > 0 {
			role = auth.Role(strings.ToLower(key.Permissions[0]))
		}
		setUserContext(c, key.UserID, key.Name, "", []string{string(role)}, nil, "apikey")
		c.Set(ContextKeyAPIKey, key)
		c.Next()
	}
}

func BasicAuthMiddleware(credentials map[string]string) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.Header("WWW-Authenticate", `Basic realm="nexuspipe"`)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authorization required"})
			return
		}

		if err := basicAuthHandler(c, authHeader); err != nil {
			c.Header("WWW-Authenticate", `Basic realm="nexuspipe"`)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		c.Next()
	}
}

func RequirePermission(enforcer *auth.RBACEnforcer, resource auth.Resource, action auth.Action) gin.HandlerFunc {
	return func(c *gin.Context) {
		roles, exists := c.Get(ContextKeyRoles)
		if !exists {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "no roles found in context"})
			return
		}

		roleList, ok := roles.([]string)
		if !ok {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "invalid roles in context"})
			return
		}

		for _, roleStr := range roleList {
			if enforcer.CheckPermission(auth.Role(roleStr), resource, action) {
				c.Next()
				return
			}
		}

		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
			"error": "insufficient permissions",
		})
	}
}

func setUserContext(c *gin.Context, userID, username, email string, roles []string, permissions []auth.Permission, authMethod string) {
	c.Set(ContextKeyUserID, userID)
	c.Set(ContextKeyUsername, username)
	c.Set(ContextKeyEmail, email)
	c.Set(ContextKeyRoles, roles)
	c.Set(ContextKeyPermissions, permissions)
	c.Set(ContextKeyAuthMethod, authMethod)
}

func GetUserID(c *gin.Context) string {
	v, _ := c.Get(ContextKeyUserID)
	id, _ := v.(string)
	return id
}

func GetUsername(c *gin.Context) string {
	v, _ := c.Get(ContextKeyUsername)
	name, _ := v.(string)
	return name
}

func GetUserEmail(c *gin.Context) string {
	v, _ := c.Get(ContextKeyEmail)
	email, _ := v.(string)
	return email
}

func GetUserRoles(c *gin.Context) []string {
	v, _ := c.Get(ContextKeyRoles)
	roles, _ := v.([]string)
	return roles
}

func GetAuthMethod(c *gin.Context) string {
	v, _ := c.Get(ContextKeyAuthMethod)
	method, _ := v.(string)
	return method
}

func GetClaims(c *gin.Context) *auth.Claims {
	v, _ := c.Get(ContextKeyClaims)
	claims, _ := v.(*auth.Claims)
	return claims
}

func GetAPIKey(c *gin.Context) *auth.APIKey {
	v, _ := c.Get(ContextKeyAPIKey)
	key, _ := v.(*auth.APIKey)
	return key
}

func basicAuthHandler(c *gin.Context, authHeader string) error {
	encoded := strings.TrimPrefix(authHeader, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return errors.New("invalid base64 encoding in Basic auth")
	}

	pair := strings.SplitN(string(decoded), ":", 2)
	if len(pair) != 2 {
		return errors.New("invalid Basic auth format")
	}

	setUserContext(c, pair[0], pair[0], "", []string{string(auth.RoleViewer)}, nil, "basic")
	return nil
}
