package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

type Serializer int

const (
	SerializerJSON Serializer = iota
	SerializerMsgpack
)

type RedisCache struct {
	client     *redis.Client
	serializer Serializer
	defaultTTL time.Duration
	prefix     string
}

type RedisCacheConfig struct {
	Address      string
	Password     string
	DB           int
	PoolSize     int
	MinIdle      int
	MaxRetries   int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	DefaultTTL   time.Duration
	KeyPrefix    string
	Serializer   Serializer
}

func DefaultRedisCacheConfig() RedisCacheConfig {
	return RedisCacheConfig{
		Address:      "localhost:6379",
		DB:           0,
		PoolSize:     10,
		MinIdle:      3,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		DefaultTTL:   5 * time.Minute,
		KeyPrefix:    "nexuspipe",
		Serializer:   SerializerMsgpack,
	}
}

func NewRedisCache(cfg RedisCacheConfig) *RedisCache {
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Address,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdle,
		MaxRetries:   cfg.MaxRetries,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})
	return &RedisCache{
		client:     rdb,
		serializer: cfg.Serializer,
		defaultTTL: cfg.DefaultTTL,
		prefix:     cfg.KeyPrefix,
	}
}

func (c *RedisCache) Client() *redis.Client {
	return c.client
}

func (c *RedisCache) key(k string) string {
	if c.prefix == "" {
		return k
	}
	return c.prefix + ":" + k
}

func (c *RedisCache) serialize(v interface{}) ([]byte, error) {
	switch c.serializer {
	case SerializerJSON:
		return json.Marshal(v)
	case SerializerMsgpack:
		return msgpack.Marshal(v)
	default:
		return json.Marshal(v)
	}
}

func (c *RedisCache) deserialize(data []byte, v interface{}) error {
	switch c.serializer {
	case SerializerJSON:
		return json.Unmarshal(data, v)
	case SerializerMsgpack:
		return msgpack.Unmarshal(data, v)
	default:
		return json.Unmarshal(data, v)
	}
}

func (c *RedisCache) Get(ctx context.Context, key string, dest interface{}) error {
	data, err := c.client.Get(ctx, c.key(key)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("cache miss: %s", key)
		}
		return fmt.Errorf("get %s: %w", key, err)
	}
	if err := c.deserialize(data, dest); err != nil {
		return fmt.Errorf("deserialize %s: %w", key, err)
	}
	return nil
}

func (c *RedisCache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	data, err := c.serialize(value)
	if err != nil {
		return fmt.Errorf("serialize %s: %w", key, err)
	}
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	if err := c.client.Set(ctx, c.key(key), data, ttl).Err(); err != nil {
		return fmt.Errorf("set %s: %w", key, err)
	}
	return nil
}

func (c *RedisCache) Delete(ctx context.Context, keys ...string) error {
	prefixed := make([]string, len(keys))
	for i, k := range keys {
		prefixed[i] = c.key(k)
	}
	if err := c.client.Del(ctx, prefixed...).Err(); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	return nil
}

func (c *RedisCache) Exists(ctx context.Context, key string) (bool, error) {
	n, err := c.client.Exists(ctx, c.key(key)).Result()
	if err != nil {
		return false, fmt.Errorf("exists %s: %w", key, err)
	}
	return n > 0, nil
}

func (c *RedisCache) TTL(ctx context.Context, key string) (time.Duration, error) {
	ttl, err := c.client.TTL(ctx, c.key(key)).Result()
	if err != nil {
		return 0, fmt.Errorf("ttl %s: %w", key, err)
	}
	return ttl, nil
}

func (c *RedisCache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if err := c.client.Expire(ctx, c.key(key), ttl).Err(); err != nil {
		return fmt.Errorf("expire %s: %w", key, err)
	}
	return nil
}

func (c *RedisCache) Close() error {
	return c.client.Close()
}

func (c *RedisCache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *RedisCache) Flush(ctx context.Context) error {
	if err := c.client.FlushAll(ctx).Err(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

func (c *RedisCache) SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) (bool, error) {
	data, err := c.serialize(value)
	if err != nil {
		return false, fmt.Errorf("serialize %s: %w", key, err)
	}
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	ok, err := c.client.SetNX(ctx, c.key(key), data, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("setnx %s: %w", key, err)
	}
	return ok, nil
}

func (c *RedisCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	fullPattern := c.key(pattern)
	keys, err := c.client.Keys(ctx, fullPattern).Result()
	if err != nil {
		return nil, fmt.Errorf("keys %s: %w", pattern, err)
	}
	return keys, nil
}
