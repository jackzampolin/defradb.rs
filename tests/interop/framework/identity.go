package framework

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/mr-tron/base58"
)

// TestIdentity holds key material and derived tokens for a test user.
type TestIdentity struct {
	PrivateKeyHex string // hex-encoded ed25519 private key (64 bytes = seed+pub)
	DID           string // did:key:z... derived from public key
	Token         string // JWT Bearer token for HTTP requests
}

// AuthHeader returns the Authorization header value for this identity.
func (id *TestIdentity) AuthHeader() string {
	return "Bearer " + id.Token
}

// ForAudience returns a copy of this identity with a JWT minted for the given audience.
// Use this when different nodes have different listen addresses.
func (id *TestIdentity) ForAudience(audience string) (*TestIdentity, error) {
	privBytes, err := hex.DecodeString(id.PrivateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode private key hex: %w", err)
	}
	priv := ed25519.PrivateKey(privBytes)
	pub := priv.Public().(ed25519.PublicKey)

	token, err := mintJWT(priv, pub, id.DID, audience)
	if err != nil {
		return nil, fmt.Errorf("failed to mint JWT for audience %s: %w", audience, err)
	}

	return &TestIdentity{
		PrivateKeyHex: id.PrivateKeyHex,
		DID:           id.DID,
		Token:         token,
	}, nil
}

// GenerateIdentity creates a new ed25519 identity with a JWT token.
// The audience parameter is used as the JWT "aud" claim (typically the node URL).
func GenerateIdentity(audience string) (*TestIdentity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate ed25519 key: %w", err)
	}

	privHex := hex.EncodeToString(priv)

	did, err := pubKeyToDID(pub)
	if err != nil {
		return nil, fmt.Errorf("failed to compute DID: %w", err)
	}

	token, err := mintJWT(priv, pub, did, audience)
	if err != nil {
		return nil, fmt.Errorf("failed to mint JWT: %w", err)
	}

	return &TestIdentity{
		PrivateKeyHex: privHex,
		DID:           did,
		Token:         token,
	}, nil
}

// pubKeyToDID converts an ed25519 public key to a did:key identifier.
// Format: did:key:z + base58btc(multicodec_ed25519_prefix + public_key)
// Multicodec ed25519-pub prefix: 0xed01 (2 bytes)
func pubKeyToDID(pub ed25519.PublicKey) (string, error) {
	// Multicodec prefix for ed25519-pub: 0xed 0x01
	prefixed := make([]byte, 2+len(pub))
	prefixed[0] = 0xed
	prefixed[1] = 0x01
	copy(prefixed[2:], pub)

	encoded := base58.Encode(prefixed)
	return "did:key:z" + encoded, nil
}

// mintJWT creates a signed JWT for the given identity.
func mintJWT(priv ed25519.PrivateKey, pub ed25519.PublicKey, did, audience string) (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":      hex.EncodeToString(pub),
		"iss":      did,
		"aud":      jwt.ClaimStrings{audience},
		"iat":      now.Unix(),
		"nbf":      now.Add(-1 * time.Minute).Unix(),
		"exp":      now.Add(1 * time.Hour).Unix(),
		"key_type": "ed25519",
	}

	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	signed, err := token.SignedString(priv)
	if err != nil {
		return "", fmt.Errorf("failed to sign JWT: %w", err)
	}
	return signed, nil
}
