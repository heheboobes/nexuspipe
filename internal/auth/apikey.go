package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

var (
	ErrInvalidAPIKey     = errors.New("invalid API key")
	ErrAPIKeyRevoked     = errors.New("API key has been revoked")
	ErrAPIKeyExpired     = errors.New("API key has expired")
	ErrKeyPrefixInvalid  = errors.New("invalid API key prefix")
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
)

const (
	APIKeyPrefix = "nxp_"
	APIKeyLength = 48
	BcryptCost   = 12
)

type APIKey struct {
	ID          string    `json:"id"`
	KeyPrefix   string    `json:"key_prefix"`
	KeyHash     string    `json:"key_hash,omitempty"`
	Name        string    `json:"name"`
	UserID      string    `json:"user_id"`
	Permissions []string  `json:"permissions"`
	Revoked     bool      `json:"revoked"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	LastUsedAt  time.Time `json:"last_used_at,omitempty"`
}

type rateLimitEntry struct {
	count       int
	windowStart time.Time
}

type APIKeyManager struct {
	mu              sync.RWMutex
	keys            map[string]*APIKey
	rateLimits      map[string]*rateLimitEntry
	rateLimitRate   int
	rateLimitWindow time.Duration
	storage         APIKeyStorage
}

type APIKeyStorage interface {
	Save(key *APIKey) error
	FindByID(id string) (*APIKey, error)
	FindByUserID(userID string) ([]*APIKey, error)
	FindByPrefix(prefix string) (*APIKey, error)
	Revoke(id string) error
	Delete(id string) error
}

type MemoryStorage struct {
	mu   sync.RWMutex
	keys map[string]*APIKey
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		keys: make(map[string]*APIKey),
	}
}

func (s *MemoryStorage) Save(key *APIKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[key.ID] = key
	return nil
}

func (s *MemoryStorage) FindByID(id string) (*APIKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key, ok := s.keys[id]
	if !ok {
		return nil, fmt.Errorf("API key not found: %s", id)
	}
	return key, nil
}

func (s *MemoryStorage) FindByUserID(userID string) ([]*APIKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*APIKey
	for _, k := range s.keys {
		if k.UserID == userID {
			result = append(result, k)
		}
	}
	return result, nil
}

func (s *MemoryStorage) FindByPrefix(prefix string) (*APIKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, k := range s.keys {
		if k.KeyPrefix == prefix {
			return k, nil
		}
	}
	return nil, fmt.Errorf("API key not found with prefix: %s", prefix)
}

func (s *MemoryStorage) Revoke(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, ok := s.keys[id]
	if !ok {
		return fmt.Errorf("API key not found: %s", id)
	}
	key.Revoked = true
	return nil
}

func (s *MemoryStorage) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.keys, id)
	return nil
}

func NewAPIKeyManager(storage APIKeyStorage, opts ...APIKeyManagerOption) *APIKeyManager {
	m := &APIKeyManager{
		keys:            make(map[string]*APIKey),
		rateLimits:      make(map[string]*rateLimitEntry),
		rateLimitRate:   100,
		rateLimitWindow: 1 * time.Minute,
		storage:         storage,
	}
	if m.storage == nil {
		m.storage = NewMemoryStorage()
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

type APIKeyManagerOption func(*APIKeyManager)

func WithRateLimit(rate int, window time.Duration) APIKeyManagerOption {
	return func(m *APIKeyManager) {
		m.rateLimitRate = rate
		m.rateLimitWindow = window
	}
}

func (m *APIKeyManager) GenerateKey(userID, name string, permissions []string, expiresAt time.Time) (string, *APIKey, error) {
	rawKey := generateRandomKey(APIKeyLength)
	prefix := APIKeyPrefix + rawKey[:8]

	hashedKey, err := hashKey(rawKey)
	if err != nil {
		return "", nil, fmt.Errorf("failed to hash API key: %w", err)
	}

	apiKey := &APIKey{
		ID:          generateKeyID(),
		KeyPrefix:   prefix,
		KeyHash:     hashedKey,
		Name:        name,
		UserID:      userID,
		Permissions: permissions,
		Revoked:     false,
		ExpiresAt:   expiresAt,
		CreatedAt:   time.Now(),
	}

	if err := m.storage.Save(apiKey); err != nil {
		return "", nil, fmt.Errorf("failed to store API key: %w", err)
	}

	fullKey := prefix + rawKey[8:]
	return fullKey, apiKey, nil
}

func (m *APIKeyManager) ValidateKey(fullKey string) (*APIKey, error) {
	if !strings.HasPrefix(fullKey, APIKeyPrefix) {
		return nil, ErrKeyPrefixInvalid
	}

	prefix := fullKey[:len(APIKeyPrefix)+8]
	apiKey, err := m.storage.FindByPrefix(prefix)
	if err != nil {
		return nil, ErrInvalidAPIKey
	}

	if apiKey.Revoked {
		return nil, ErrAPIKeyRevoked
	}

	if !apiKey.ExpiresAt.IsZero() && time.Now().After(apiKey.ExpiresAt) {
		return nil, ErrAPIKeyExpired
	}

	rawSecret := fullKey[len(prefix):]
	if err := verifyKey(fullKey, apiKey.KeyHash); err != nil {
		if err2 := verifyKey(rawSecret, apiKey.KeyHash); err2 != nil {
			return nil, ErrInvalidAPIKey
		}
	}

	apiKey.LastUsedAt = time.Now()
	m.storage.Save(apiKey)

	return apiKey, nil
}

func (m *APIKeyManager) RevokeKey(id string) error {
	return m.storage.Revoke(id)
}

func (m *APIKeyManager) GetKey(id string) (*APIKey, error) {
	return m.storage.FindByID(id)
}

func (m *APIKeyManager) GetUserKeys(userID string) ([]*APIKey, error) {
	return m.storage.FindByUserID(userID)
}

func (m *APIKeyManager) DeleteKey(id string) error {
	return m.storage.Delete(id)
}

func (m *APIKeyManager) CheckRateLimit(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, exists := m.rateLimits[key]
	now := time.Now()

	if !exists || now.Sub(entry.windowStart) > m.rateLimitWindow {
		m.rateLimits[key] = &rateLimitEntry{
			count:       1,
			windowStart: now,
		}
		return nil
	}

	entry.count++
	if entry.count > m.rateLimitRate {
		return ErrRateLimitExceeded
	}
	return nil
}

func hashKey(key string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(key), BcryptCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

func verifyKey(key, hash string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(key))
}

func generateRandomKey(length int) string {
	b := make([]byte, length)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func generateKeyID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func SanitizeAPIKey(fullKey string) string {
	if len(fullKey) <= 12 {
		return "***"
	}
	return fullKey[:12] + "***"
}

func ComputeKeyFingerprint(fullKey string) string {
	h := sha256.Sum256([]byte(fullKey))
	return base64.StdEncoding.EncodeToString(h[:])
}
