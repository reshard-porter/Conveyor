package pgwire

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// scramClient implements the client side of SCRAM-SHA-256 (RFC 5802/7677)
// as used by PostgreSQL (no channel binding).
type scramClient struct {
	password    string
	clientNonce string
	firstBare   string // client-first-message-bare
	authMessage string
	saltedPw    []byte
}

func newScramClient(password string) *scramClient {
	raw := make([]byte, 18)
	_, _ = rand.Read(raw)
	return &scramClient{password: password, clientNonce: base64.RawStdEncoding.EncodeToString(raw)}
}

func (s *scramClient) clientFirstMessage() string { return "n,," + s.firstBare }

func (s *scramClient) clientFirst() []byte {
	// PostgreSQL ignores the username here and uses the startup user.
	s.firstBare = "n=,r=" + s.clientNonce
	return []byte(s.clientFirstMessage())
}

func (s *scramClient) clientFinal(serverFirst []byte) ([]byte, error) {
	attrs := parseScramAttrs(string(serverFirst))
	nonce, salt64, iterStr := attrs["r"], attrs["s"], attrs["i"]
	if !strings.HasPrefix(nonce, s.clientNonce) {
		return nil, errors.New("pgwire: scram: server nonce does not extend client nonce")
	}
	salt, err := base64.StdEncoding.DecodeString(salt64)
	if err != nil {
		return nil, fmt.Errorf("pgwire: scram: bad salt: %w", err)
	}
	iter, err := strconv.Atoi(iterStr)
	if err != nil || iter < 1 {
		return nil, errors.New("pgwire: scram: bad iteration count")
	}
	s.saltedPw, err = pbkdf2.Key(sha256.New, s.password, salt, iter, sha256.Size)
	if err != nil {
		return nil, err
	}
	withoutProof := "c=biws,r=" + nonce
	s.authMessage = s.firstBare + "," + string(serverFirst) + "," + withoutProof

	clientKey := hmacSHA256(s.saltedPw, "Client Key")
	storedKey := sha256.Sum256(clientKey)
	clientSig := hmacSHA256(storedKey[:], s.authMessage)
	proof := make([]byte, len(clientKey))
	for i := range clientKey {
		proof[i] = clientKey[i] ^ clientSig[i]
	}
	return []byte(withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)), nil
}

func (s *scramClient) verifyServer(serverFinal []byte) error {
	attrs := parseScramAttrs(string(serverFinal))
	if e, ok := attrs["e"]; ok {
		return fmt.Errorf("pgwire: scram: server error: %s", e)
	}
	got, err := base64.StdEncoding.DecodeString(attrs["v"])
	if err != nil {
		return errors.New("pgwire: scram: bad server signature")
	}
	serverKey := hmacSHA256(s.saltedPw, "Server Key")
	want := hmacSHA256(serverKey, s.authMessage)
	if !hmac.Equal(got, want) {
		return errors.New("pgwire: scram: server signature mismatch")
	}
	return nil
}

func hmacSHA256(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msg))
	return h.Sum(nil)
}

func parseScramAttrs(s string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		if k, v, ok := strings.Cut(part, "="); ok {
			out[k] = v
		}
	}
	return out
}
