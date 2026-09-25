package identity

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)

var (
	ErrEmptyID   = errors.New("identity id must not be empty")
	ErrInvalidID = errors.New("identity id must be a canonical UUID")
)

type Kind string

const (
	ParticipantKind Kind = "participant"
	LogicalNodeKind Kind = "logical-node"
)

type Identity struct {
	ID   string
	Kind Kind
}

func New(kind Kind) (Identity, error) {
	if !validKind(kind) {
		return Identity{}, fmt.Errorf("%w: invalid kind %q", ErrInvalidID, kind)
	}

	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return Identity{}, fmt.Errorf("generate identity: %w", err)
	}

	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80

	id := fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(bytes[0:4]),
		hex.EncodeToString(bytes[4:6]),
		hex.EncodeToString(bytes[6:8]),
		hex.EncodeToString(bytes[8:10]),
		hex.EncodeToString(bytes[10:16]),
	)

	return Identity{ID: id, Kind: kind}, nil
}

func Parse(id string, kind Kind) (Identity, error) {
	if id == "" {
		return Identity{}, ErrEmptyID
	}
	if !validKind(kind) {
		return Identity{}, fmt.Errorf("%w: invalid kind %q", ErrInvalidID, kind)
	}
	if !isCanonicalUUID(id) {
		return Identity{}, fmt.Errorf("%w: %q", ErrInvalidID, id)
	}
	return Identity{ID: id, Kind: kind}, nil
}

func (i Identity) Validate() error {
	if i.ID == "" {
		return ErrEmptyID
	}
	if !validKind(i.Kind) {
		return fmt.Errorf("%w: invalid kind %q", ErrInvalidID, i.Kind)
	}
	if !isCanonicalUUID(i.ID) {
		return fmt.Errorf("%w: %q", ErrInvalidID, i.ID)
	}
	return nil
}

func validKind(kind Kind) bool {
	return kind == ParticipantKind || kind == LogicalNodeKind
}

func isCanonicalUUID(id string) bool {
	if len(id) != 36 {
		return false
	}

	for i := 0; i < len(id); i++ {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if id[i] != '-' {
				return false
			}
			continue
		}
		if !isHex(id[i]) {
			return false
		}
	}

	return id[14] == '4' && (id[19] == '8' || id[19] == '9' || id[19] == 'a' || id[19] == 'b')
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') ||
		(c >= 'a' && c <= 'f')
}
