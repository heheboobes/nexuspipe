package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type CORSMode string

const (
	CORSModeAllowAll   CORSMode = "allow-all"
	CORSModeWhitelist  CORSMode = "whitelist"
	CORSModeSameOrigin CORSMode = "same-origin"
)

type CORSConfig struct {
	AllowedOrigins   []string
	AllowedMethods   []string
	AllowedHeaders   []string
	ExposedHeaders   []string
	AllowCredentials bool
	MaxAge           time.Duration
	Mode             CORSMode
}

func DefaultCORSConfig() CORSConfig {
	return CORSConfig{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Origin", "Content-Type", "Accept", "Authorization", "X-API-Key", "X-Request-ID"},
		ExposedHeaders:   []string{"Content-Length", "Content-Type", "X-Request-ID", "X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
		Mode:             CORSModeAllowAll,
	}
}

func CORS(config CORSConfig) gin.HandlerFunc {
	config = normalizeConfig(config)

	allowMethods := strings.Join(config.AllowedMethods, ", ")
	allowHeaders := strings.Join(config.AllowedHeaders, ", ")
	exposeHeaders := strings.Join(config.ExposedHeaders, ", ")
	maxAge := strconv.Itoa(int(config.MaxAge.Seconds()))

	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")

		if origin == "" {
			c.Next()
			return
		}

		if !isOriginAllowed(origin, config) {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}

		c.Header("Access-Control-Allow-Origin", resolveOrigin(origin, config))
		c.Header("Access-Control-Allow-Methods", allowMethods)
		c.Header("Access-Control-Allow-Headers", allowHeaders)
		c.Header("Access-Control-Expose-Headers", exposeHeaders)
		c.Header("Access-Control-Max-Age", maxAge)

		if config.AllowCredentials {
			c.Header("Access-Control-Allow-Credentials", "true")
		}

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

func CORSMiddleware(allowedOrigins []string) gin.HandlerFunc {
	config := DefaultCORSConfig()
	if len(allowedOrigins) > 0 {
		config.AllowedOrigins = allowedOrigins
		config.Mode = CORSModeWhitelist
	}
	return CORS(config)
}

func CORSAllowAll() gin.HandlerFunc {
	return CORS(DefaultCORSConfig())
}

func CORSWithConfig(origins []string, methods []string, headers []string, credentials bool) gin.HandlerFunc {
	config := DefaultCORSConfig()
	if len(origins) > 0 {
		config.AllowedOrigins = origins
		config.Mode = CORSModeWhitelist
	}
	if len(methods) > 0 {
		config.AllowedMethods = methods
	}
	if len(headers) > 0 {
		config.AllowedHeaders = headers
	}
	config.AllowCredentials = credentials
	return CORS(config)
}

func normalizeConfig(config CORSConfig) CORSConfig {
	if len(config.AllowedOrigins) == 0 {
		config.AllowedOrigins = []string{"*"}
	}
	if len(config.AllowedMethods) == 0 {
		config.AllowedMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}
	}
	if len(config.AllowedHeaders) == 0 {
		config.AllowedHeaders = []string{"Origin", "Content-Type", "Accept", "Authorization"}
	}
	if config.MaxAge == 0 {
		config.MaxAge = 12 * time.Hour
	}
	return config
}

func isOriginAllowed(origin string, config CORSConfig) bool {
	switch config.Mode {
	case CORSModeAllowAll:
		return true
	case CORSModeSameOrigin:
		return isSameOrigin(origin)
	default:
		for _, allowed := range config.AllowedOrigins {
			if allowed == "*" {
				return true
			}
			if matchOrigin(origin, allowed) {
				return true
			}
		}
		return false
	}
}

func resolveOrigin(origin string, config CORSConfig) string {
	if config.Mode == CORSModeAllowAll && !config.AllowCredentials {
		return "*"
	}

	for _, allowed := range config.AllowedOrigins {
		if allowed == "*" {
			return origin
		}
		if matchOrigin(origin, allowed) {
			return origin
		}
	}

	if config.Mode == CORSModeAllowAll {
		return origin
	}

	return config.AllowedOrigins[0]
}

func matchOrigin(origin, allowed string) bool {
	if allowed == "*" {
		return true
	}
	if allowed == origin {
		return true
	}
	if strings.HasPrefix(allowed, "https://*.") {
		suffix := strings.TrimPrefix(allowed, "https://*.")
		return strings.HasSuffix(origin, "."+suffix) || origin == "https://"+suffix
	}
	if strings.HasPrefix(allowed, "http://*.") {
		suffix := strings.TrimPrefix(allowed, "http://*.")
		return strings.HasSuffix(origin, "."+suffix) || origin == "http://"+suffix
	}
	if strings.Contains(allowed, "*") {
		pattern := strings.ReplaceAll(allowed, "*", "[^/]+")
		return strings.Contains(origin, pattern)
	}
	return false
}

func isSameOrigin(origin string) bool {
	return strings.HasPrefix(origin, "http://localhost") ||
		strings.HasPrefix(origin, "https://localhost") ||
		strings.HasPrefix(origin, "http://127.0.0.1") ||
		origin == ""
}

func buildVaryHeader(c *gin.Context) {
	vary := c.Writer.Header().Get("Vary")
	if vary == "" {
		c.Header("Vary", "Origin")
	} else if !strings.Contains(vary, "Origin") {
		c.Header("Vary", vary+", Origin")
	}
}

func handlePreflight(c *gin.Context, config CORSConfig) bool {
	if c.Request.Method != http.MethodOptions {
		return false
	}

	origin := c.Request.Header.Get("Origin")
	reqMethod := c.Request.Header.Get("Access-Control-Request-Method")

	if origin == "" || reqMethod == "" {
		c.AbortWithStatus(http.StatusBadRequest)
		return true
	}

	if !isOriginAllowed(origin, config) {
		c.AbortWithStatus(http.StatusForbidden)
		return true
	}

	methodOk := false
	for _, m := range config.AllowedMethods {
		if strings.EqualFold(m, reqMethod) {
			methodOk = true
			break
		}
	}
	if !methodOk {
		c.AbortWithStatus(http.StatusForbidden)
		return true
	}

	reqHeaders := c.Request.Header.Get("Access-Control-Request-Headers")
	if reqHeaders != "" {
		requestedHeaders := strings.Split(reqHeaders, ",")
		for _, rh := range requestedHeaders {
			rh = strings.TrimSpace(rh)
			found := false
			for _, ah := range config.AllowedHeaders {
				if strings.EqualFold(ah, rh) {
					found = true
					break
				}
			}
			if !found {
				c.AbortWithStatus(http.StatusForbidden)
				return true
			}
		}
	}

	return false
}
