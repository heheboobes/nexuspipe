package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrTokenExpired     = errors.New("token has expired")
	ErrTokenInvalid     = errors.New("token is invalid")
	ErrTokenMalformed   = errors.New("token is malformed")
	ErrTokenNotValidYet = errors.New("token is not valid yet")
)

type Permission string

const (
	PermissionRead   Permission = "read"
	PermissionWrite  Permission = "write"
	PermissionDelete Permission = "delete"
	PermissionAdmin  Permission = "admin"
)

type Claims struct {
	jwt.RegisteredClaims
	UserID      string       `json:"user_id"`
	Username    string       `json:"username"`
	Email       string       `json:"email"`
	Roles       []string     `json:"roles"`
	Permissions []Permission `json:"permissions"`
	TokenType   string       `json:"token_type"`
}

type JWTManager struct {
	secretKey     []byte
	privateKey    *rsa.PrivateKey
	publicKey     *rsa.PublicKey
	signingMethod string
	accessExpiry  time.Duration
	refreshExpiry time.Duration
	issuer        string
}

type JWTManagerOption func(*JWTManager)

func WithHMACSecret(secret string) JWTManagerOption {
	return func(m *JWTManager) {
		m.secretKey = []byte(secret)
		m.signingMethod = "HS256"
	}
}

func WithRSAKeys(privatePath, publicPath string) JWTManagerOption {
	return func(m *JWTManager) {
		privPEM, err := os.ReadFile(privatePath)
		if err != nil {
			panic(fmt.Sprintf("failed to read private key: %v", err))
		}
		block, _ := pem.Decode(privPEM)
		if block == nil {
			panic("failed to decode private key PEM")
		}
		priv, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			priv, err = x509.ParsePKCS1PrivateKey(block.Bytes)
			if err != nil {
				panic(fmt.Sprintf("failed to parse private key: %v", err))
			}
		}
		var ok bool
		m.privateKey, ok = priv.(*rsa.PrivateKey)
		if !ok {
			panic("private key is not RSA")
		}
		m.publicKey = &m.privateKey.PublicKey

		pubPEM, err := os.ReadFile(publicPath)
		if err != nil {
			return
		}
		block, _ = pem.Decode(pubPEM)
		if block != nil {
			pub, parseErr := x509.ParsePKIXPublicKey(block.Bytes)
			if parseErr == nil {
				if rsaPub, ok := pub.(*rsa.PublicKey); ok {
					m.publicKey = rsaPub
				}
			}
		}
		m.signingMethod = "RS256"
	}
}

func WithAccessExpiry(d time.Duration) JWTManagerOption {
	return func(m *JWTManager) {
		m.accessExpiry = d
	}
}

func WithRefreshExpiry(d time.Duration) JWTManagerOption {
	return func(m *JWTManager) {
		m.refreshExpiry = d
	}
}

func WithIssuer(issuer string) JWTManagerOption {
	return func(m *JWTManager) {
		m.issuer = issuer
	}
}

func NewJWTManager(opts ...JWTManagerOption) *JWTManager {
	m := &JWTManager{
		secretKey:     []byte("nexuspipe-default-secret-change-in-production"),
		signingMethod: "HS256",
		accessExpiry:  15 * time.Minute,
		refreshExpiry: 7 * 24 * time.Hour,
		issuer:        "nexuspipe",
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *JWTManager) GenerateToken(userID, username, email string, roles []string, permissions []Permission) (string, error) {
	now := time.Now()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        generateJTI(),
			Subject:   userID,
			Issuer:    m.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.accessExpiry)),
		},
		UserID:      userID,
		Username:    username,
		Email:       email,
		Roles:       roles,
		Permissions: permissions,
		TokenType:   "access",
	}

	var token *jwt.Token
	switch m.signingMethod {
	case "RS256":
		token = jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	default:
		token = jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	}

	tokenString, err := token.SignedString(m.signingKey())
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}
	return tokenString, nil
}

func (m *JWTManager) ValidateToken(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, m.keyFunc,
		jwt.WithLeeway(30*time.Second),
		jwt.WithValidMethods(m.validMethods()),
	)
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrTokenExpired
		}
		if errors.Is(err, jwt.ErrTokenNotValidYet) {
			return nil, ErrTokenNotValidYet
		}
		if errors.Is(err, jwt.ErrTokenMalformed) {
			return nil, ErrTokenMalformed
		}
		return nil, fmt.Errorf("%w: %v", ErrTokenInvalid, err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, ErrTokenInvalid
	}
	return claims, nil
}

func (m *JWTManager) RefreshToken(tokenString string) (string, error) {
	claims, err := m.ValidateToken(tokenString)
	if err != nil {
		return "", err
	}

	if claims.TokenType != "access" {
		return "", errors.New("only access tokens can be refreshed")
	}

	sinceIssued := time.Since(claims.IssuedAt.Time)
	if sinceIssued > m.refreshExpiry {
		return "", ErrTokenExpired
	}

	return m.GenerateToken(claims.UserID, claims.Username, claims.Email, claims.Roles, claims.Permissions)
}

func (m *JWTManager) GenerateRefreshToken(userID, username, email string, roles []string, permissions []Permission) (string, error) {
	now := time.Now()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        generateJTI(),
			Subject:   userID,
			Issuer:    m.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.refreshExpiry)),
		},
		UserID:      userID,
		Username:    username,
		Email:       email,
		Roles:       roles,
		Permissions: permissions,
		TokenType:   "refresh",
	}

	var token *jwt.Token
	switch m.signingMethod {
	case "RS256":
		token = jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	default:
		token = jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	}

	tokenString, err := token.SignedString(m.signingKey())
	if err != nil {
		return "", fmt.Errorf("failed to sign refresh token: %w", err)
	}
	return tokenString, nil
}

func (m *JWTManager) ParseTokenUnverified(tokenString string) (*Claims, error) {
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, &Claims{})
	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}
	claims, ok := token.Claims.(*Claims)
	claims.IssuedAt = nil
	if !ok {
		return nil, ErrTokenMalformed
	}
	return claims, nil
}

func (m *JWTManager) signingKey() interface{} {
	switch m.signingMethod {
	case "RS256":
		return m.privateKey
	default:
		return m.secretKey
	}
}

func (m *JWTManager) keyFunc(token *jwt.Token) (interface{}, error) {
	switch m.signingMethod {
	case "RS256":
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return m.publicKey, nil
	default:
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return m.secretKey, nil
	}
}

func (m *JWTManager) validMethods() []string {
	switch m.signingMethod {
	case "RS256":
		return []string{"RS256"}
	default:
		return []string{"HS256"}
	}
}

func generateJTI() string {
	b := make([]byte, 16)
	rand.Read(b)
	h := hmac.New(sha256.New, b)
	h.Write([]byte(time.Now().String()))
	return strings.TrimRight(base64.URLEncoding.EncodeToString(h.Sum(nil)), "=")
}

func ExtractBearerToken(authHeader string) (string, error) {
	if authHeader == "" {
		return "", errors.New("authorization header is missing")
	}
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return "", errors.New("invalid authorization header format, expected 'Bearer <token>'")
	}
	return parts[1], nil
}

func MarshalPermissions(perms []Permission) (string, error) {
	data, err := json.Marshal(perms)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func UnmarshalPermissions(data string) ([]Permission, error) {
	var perms []Permission
	if err := json.Unmarshal([]byte(data), &perms); err != nil {
		return nil, err
	}
	return perms, nil
}
