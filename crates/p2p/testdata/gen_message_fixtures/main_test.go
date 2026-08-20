package main

import (
	"testing"

	"github.com/fxamacker/cbor/v2"
)

// Rust Iroh publishers add endpoint-origin authentication to the lightweight
// gossip shape. Go's existing PushLogRequest decoder must keep accepting the
// core head hint during a rolling mixed deployment and ignore those additive
// fields; it must not treat SourcePeerID as authenticated message metadata.
func TestPushLogOriginEnvelopeIsAdditiveForGo(t *testing.T) {
	type pushLogBroadcastWithOrigin struct {
		DocID           string
		CID             []byte
		CollectionID    string
		Creator         string
		Block           []byte
		SourcePeerID    string
		OriginSignature []byte
	}

	wire, err := cbor.Marshal(pushLogBroadcastWithOrigin{
		DocID:           "bafy-doc",
		CID:             []byte{0x01, 0x71, 0xaa},
		CollectionID:    "bafy-collection",
		Creator:         "did:key:zRust",
		Block:           []byte{0xa1, 0x61, 0x78, 0x01},
		SourcePeerID:    "iroh-endpoint-id",
		OriginSignature: []byte{0x03, 0x04},
	})
	if err != nil {
		t.Fatal(err)
	}

	var decoded pushLogRequest
	if err := cbor.Unmarshal(wire, &decoded); err != nil {
		t.Fatalf("Go PushLogRequest rejected additive Rust origin fields: %v", err)
	}
	if decoded.DocID != "bafy-doc" || decoded.CollectionID != "bafy-collection" {
		t.Fatalf("decoded wrong head scope: %#v", decoded)
	}
	if decoded.SenderID != "" || len(decoded.Signature) != 0 {
		t.Fatalf("origin hint was confused with authenticated Go metadata: %#v", decoded.metaData)
	}
}
