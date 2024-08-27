package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type RateLimiter struct {
	mu              sync.RWMutex
	ipBuckets       map[string]*tokenBucket
	userBuckets     map[string]*tokenBucket
	globalRate      float64
	globalBurst     int
	ipRate          float64
	ipBurst         int
	userRate        float64
	userBurst       int
	cleanupInterval time.Duration
	lastCleanup     time.Time
}

type tokenBucket struct {
	tokens     float64
	lastRefill time.Time
	rate       float64
	burst      int
}

type RateLimiterConfig struct {
	GlobalRate      float64       `json:"global_rate"`
	GlobalBurst     int           `json:"global_burst"`
	IPRate          float64       `json:"ip_rate"`
	IPBurst         int           `json:"ip_burst"`
	UserRate        float64       `json:"user_rate"`
	UserBurst       int           `json:"user_burst"`
	CleanupInterval time.Duration `json:"cleanup_interval"`
}

func NewRateLimiter(config RateLimiterConfig) *RateLimiter {
	if config.IPRate == 0 {
		config.IPRate = 10
	}
	if config.IPBurst == 0 {
		config.IPBurst = 20
	}
	if config.UserRate == 0 {
		config.UserRate = 100
	}
	if config.UserBurst == 0 {
		config.UserBurst = 200
	}
	if config.GlobalRate == 0 {
		config.GlobalRate = 1000
	}
	if config.GlobalBurst == 0 {
		config.GlobalBurst = 1500
	}
	if config.CleanupInterval == 0 {
		config.CleanupInterval = 5 * time.Minute
	}

	return &RateLimiter{
		ipBuckets:       make(map[string]*tokenBucket),
		userBuckets:     make(map[string]*tokenBucket),
		globalRate:      config.GlobalRate,
		globalBurst:     config.GlobalBurst,
		ipRate:          config.IPRate,
		ipBurst:         config.IPBurst,
		userRate:        config.UserRate,
		userBurst:       config.UserBurst,
		cleanupInterval: config.CleanupInterval,
		lastCleanup:     time.Now(),
	}
}

func (rl *RateLimiter) GlobalRateLimit() gin.HandlerFunc {
	globalBucket := &tokenBucket{
		tokens: float64(rl.globalBurst),
		rate:   rl.globalRate,
		burst:  rl.globalBurst,
	}

	return func(c *gin.Context) {
		rl.refillBucket(globalBucket, time.Now())
		if globalBucket.tokens < 1 {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":       "global rate limit exceeded",
				"retry_after": rl.retryAfter(globalBucket),
			})
			return
		}
		globalBucket.tokens--
		c.Next()
	}
}

func (rl *RateLimiter) IPRateLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		rl.performCleanup()

		ip := c.ClientIP()
		bucket := rl.getOrCreateIPBucket(ip)

		rl.refillBucket(bucket, time.Now())
		if bucket.tokens < 1 {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":       "rate limit exceeded for this IP",
				"retry_after": rl.retryAfter(bucket),
			})
			return
		}
		bucket.tokens--
		c.Next()
	}
}

func (rl *RateLimiter) UserRateLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		rl.performCleanup()

		userID := GetUserID(c)
		if userID == "" {
			c.Next()
			return
		}

		bucket := rl.getOrCreateUserBucket(userID)

		rl.refillBucket(bucket, time.Now())
		if bucket.tokens < 1 {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":       "rate limit exceeded for this user",
				"retry_after": rl.retryAfter(bucket),
			})
			return
		}
		bucket.tokens--
		c.Next()
	}
}

func (rl *RateLimiter) RateLimit() gin.HandlerFunc {
	globalBucket := &tokenBucket{
		tokens: float64(rl.globalBurst),
		rate:   rl.globalRate,
		burst:  rl.globalBurst,
	}

	return func(c *gin.Context) {
		rl.performCleanup()

		rl.refillBucket(globalBucket, time.Now())
		if globalBucket.tokens < 1 {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":       "global rate limit exceeded",
				"retry_after": rl.retryAfter(globalBucket),
			})
			return
		}
		globalBucket.tokens--

		ip := c.ClientIP()
		ipBucket := rl.getOrCreateIPBucket(ip)
		rl.refillBucket(ipBucket, time.Now())
		if ipBucket.tokens < 1 {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":       "IP rate limit exceeded",
				"retry_after": rl.retryAfter(ipBucket),
			})
			return
		}
		ipBucket.tokens--

		if userID := GetUserID(c); userID != "" {
			userBucket := rl.getOrCreateUserBucket(userID)
			rl.refillBucket(userBucket, time.Now())
			if userBucket.tokens < 1 {
				c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
					"error":       "user rate limit exceeded",
					"retry_after": rl.retryAfter(userBucket),
				})
				return
			}
			userBucket.tokens--
		}

		c.Next()
	}
}

func (rl *RateLimiter) refillBucket(bucket *tokenBucket, now time.Time) {
	elapsed := now.Sub(bucket.lastRefill).Seconds()
	tokensToAdd := elapsed * bucket.rate
	bucket.tokens += tokensToAdd
	if bucket.tokens > float64(bucket.burst) {
		bucket.tokens = float64(bucket.burst)
	}
	bucket.lastRefill = now
}

func (rl *RateLimiter) retryAfter(bucket *tokenBucket) float64 {
	needed := 1 - bucket.tokens
	if needed <= 0 {
		return 0
	}
	return needed / bucket.rate
}

func (rl *RateLimiter) getOrCreateIPBucket(ip string) *tokenBucket {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	bucket, exists := rl.ipBuckets[ip]
	if !exists {
		bucket = &tokenBucket{
			tokens:     float64(rl.ipBurst),
			lastRefill: time.Now(),
			rate:       rl.ipRate,
			burst:      rl.ipBurst,
		}
		rl.ipBuckets[ip] = bucket
	}
	return bucket
}

func (rl *RateLimiter) getOrCreateUserBucket(userID string) *tokenBucket {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	bucket, exists := rl.userBuckets[userID]
	if !exists {
		bucket = &tokenBucket{
			tokens:     float64(rl.userBurst),
			lastRefill: time.Now(),
			rate:       rl.userRate,
			burst:      rl.userBurst,
		}
		rl.userBuckets[userID] = bucket
	}
	return bucket
}

func (rl *RateLimiter) performCleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if time.Since(rl.lastCleanup) < rl.cleanupInterval {
		return
	}

	now := time.Now()
	threshold := now.Add(-2 * rl.cleanupInterval)

	for ip, bucket := range rl.ipBuckets {
		if bucket.lastRefill.Before(threshold) {
			delete(rl.ipBuckets, ip)
		}
	}

	for uid, bucket := range rl.userBuckets {
		if bucket.lastRefill.Before(threshold) {
			delete(rl.userBuckets, uid)
		}
	}

	rl.lastCleanup = now
}

func (rl *RateLimiter) ResetIPBucket(ip string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(rl.ipBuckets, ip)
}

func (rl *RateLimiter) ResetUserBucket(userID string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(rl.userBuckets, userID)
}

func (rl *RateLimiter) GetIPBucketState(ip string) (tokens float64, rate float64, burst int) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	bucket, exists := rl.ipBuckets[ip]
	if !exists {
		return float64(rl.ipBurst), rl.ipRate, rl.ipBurst
	}
	return bucket.tokens, bucket.rate, bucket.burst
}

func (rl *RateLimiter) GetUserBucketState(userID string) (tokens float64, rate float64, burst int) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	bucket, exists := rl.userBuckets[userID]
	if !exists {
		return float64(rl.userBurst), rl.userRate, rl.userBurst
	}
	return bucket.tokens, bucket.rate, bucket.burst
}
