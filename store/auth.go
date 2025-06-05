package store

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// User represents a system user
type User struct {
	ID       string
	Username string
	Password string // This should be hashed in a real implementation
	Role     string
}

// Claims represents the JWT claims
type Claims struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

// AuthService handles authentication
type AuthService struct {
	jwtSecret []byte
	users     map[string]*User // In a real implementation, this would be a database
}

// NewAuthService creates a new authentication service
func NewAuthService(jwtSecret string) (*AuthService, error) {
	if len(jwtSecret) < 32 {
		return nil, errors.New("JWT secret must be at least 32 bytes")
	}

	return &AuthService{
		jwtSecret: []byte(jwtSecret),
		users:     make(map[string]*User),
	}, nil
}

// AddUser adds a new user to the system
func (a *AuthService) AddUser(username, password, role string) error {
	// In a real implementation, you would hash the password
	user := &User{
		ID:       generateID(),
		Username: username,
		Password: password, // This should be hashed
		Role:     role,
	}
	a.users[username] = user
	return nil
}

// LoginRequest represents the login request payload
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// LoginResponse represents the login response payload
type LoginResponse struct {
	Token     string `json:"token"`
	ExpiresAt int64  `json:"expires_at"`
}

// Login authenticates a user and returns a JWT token
func (a *AuthService) Login(username, password string) (*LoginResponse, error) {
	user, exists := a.users[username]
	if !exists {
		return nil, errors.New("invalid credentials")
	}

	// In a real implementation, you would compare hashed passwords
	if user.Password != password {
		return nil, errors.New("invalid credentials")
	}

	// Create the claims
	expirationTime := time.Now().Add(24 * time.Hour)
	claims := &Claims{
		UserID:   user.ID,
		Username: user.Username,
		Role:     user.Role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
			Issuer:    "haystack",
			Subject:   user.ID,
		},
	}

	// Create the token
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// Sign the token
	tokenString, err := token.SignedString(a.jwtSecret)
	if err != nil {
		return nil, fmt.Errorf("failed to sign token: %w", err)
	}

	return &LoginResponse{
		Token:     tokenString,
		ExpiresAt: expirationTime.Unix(),
	}, nil
}

// ValidateToken validates a JWT token and returns the claims
func (a *AuthService) ValidateToken(tokenString string) (*Claims, error) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return a.jwtSecret, nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	if !token.Valid {
		return nil, errors.New("invalid token")
	}

	return claims, nil
}

// generateID generates a random ID
func generateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}
