// Generate byte-exact dag-cbor fixtures for the pubsub_rpc internalResponse
// envelope, matching the encoder used by sourcenetwork/go-libp2p-pubsub-rpc:
//
//	ipld.Marshal(dagcbor.Encode, &internalResponse{...}, resType)
//
// Run: `go run main.go` — prints the hex-encoded dag-cbor bytes for two
// fixtures (non-error + error) along with the source struct fields so the
// Rust side can bake them in as parity constants.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/fxamacker/cbor/v2"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	_ "github.com/ipld/go-ipld-prime/schema"
	mh "github.com/multiformats/go-multihash"
)

type internalResponse struct {
	ID   string
	From []byte
	Data []byte
	Err  string
}

func main() {
	ts, err := ipld.LoadSchemaBytes([]byte(`
	type internalResponse struct {
		ID String
		From Bytes
		Data Bytes
		Err String
	}`))
	if err != nil {
		fmt.Fprintln(os.Stderr, "schema load:", err)
		os.Exit(1)
	}
	resType := ts.TypeByName("internalResponse")

	fixtures := []struct {
		label string
		res   internalResponse
	}{
		{
			label: "ok",
			res: internalResponse{
				ID:   "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku",
				From: []byte{0x12, 0x20, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20},
				Data: []byte("hello"),
				Err:  "",
			},
		},
		{
			label: "err",
			res: internalResponse{
				ID:   "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku",
				From: []byte{},
				Data: []byte{},
				Err:  "unknown doc",
			},
		},
	}

	for _, f := range fixtures {
		node := bindnode.Wrap(&f.res, resType)
		b, err := ipld.Encode(node.Representation(), dagcbor.Encode)
		if err != nil {
			fmt.Fprintln(os.Stderr, "encode:", err)
			os.Exit(1)
		}
		fmt.Printf("fixture: %s\n", f.label)
		fmt.Printf("  ID:   %q\n", f.res.ID)
		fmt.Printf("  From: %x\n", f.res.From)
		fmt.Printf("  Data: %x\n", f.res.Data)
		fmt.Printf("  Err:  %q\n", f.res.Err)
		fmt.Printf("  cbor (%d bytes): %s\n", len(b), hex.EncodeToString(b))

		// Verify the other end of the round-trip using schema-aware decode.
		var rt internalResponse
		builder := bindnode.Prototype(&rt, resType).Representation().NewBuilder()
		if err := dagcbor.Decode(builder, bytes.NewReader(b)); err != nil {
			fmt.Fprintln(os.Stderr, "decode:", err)
			os.Exit(1)
		}
		_ = bindnode.Unwrap(builder.Build())
		fmt.Println()
	}

	// Also emit a quick sanity dump of the first-byte major type, so the
	// Rust side can assert on the same invariant.
	for _, f := range fixtures {
		node := bindnode.Wrap(&f.res, resType)
		b, _ := ipld.Encode(node.Representation(), dagcbor.Encode)
		fmt.Printf("%s: first byte = 0x%02x (0xa4 = 4-field definite-length map)\n", f.label, b[0])
	}

	// ------------------------------------------------------------------
	// End-to-end replay fixture: a full doc-sync exchange showing exactly
	// what would travel on the wire between Go peers. Used by the Rust
	// side to validate byte-for-byte parity through the whole pipeline:
	// request bytes (on `doc-sync`) → request ID (CIDv1 raw+sha256) →
	// inner reply bytes → dag-cbor envelope (on `<base>/<peer>/_response`).
	//
	// If this output changes, update the constants in
	// `crates/p2p/src/sync/coordinator/pubsub_services.rs::tests::go_parity_*`.
	// ------------------------------------------------------------------

	type docSyncRequest struct {
		DocIDs []string `json:"docIDs"`
	}
	type docSyncItem struct {
		DocID string   `json:"docID"`
		Heads [][]byte `json:"heads"`
	}
	type docSyncReply struct {
		Results []docSyncItem `json:"results"`
		Sender  string        `json:"sender"`
	}

	req := docSyncRequest{DocIDs: []string{"docA"}}
	reqBytes, err := cbor.Marshal(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "req encode:", err)
		os.Exit(1)
	}

	// Derive request ID: CIDv1(raw, sha256(reqBytes)). Matches Go's
	// go-libp2p-pubsub-rpc `cid.NewCidV1(cid.Raw, util.Hash(data))`.
	digest := sha256.Sum256(reqBytes)
	mhash, err := mh.Encode(digest[:], mh.SHA2_256)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mh:", err)
		os.Exit(1)
	}
	requestID := cid.NewCidV1(cid.Raw, mhash)

	// Use a realistic head: CIDv1(raw, sha256("docA-head")). The Rust side
	// parses head bytes back into a `cid::Cid` before re-encoding on the wire,
	// so the fixture head must be a valid multihash-prefixed CID.
	headDigest := sha256.Sum256([]byte("docA-head"))
	headMhash, err := mh.Encode(headDigest[:], mh.SHA2_256)
	if err != nil {
		fmt.Fprintln(os.Stderr, "head mh:", err)
		os.Exit(1)
	}
	headCid := cid.NewCidV1(cid.Raw, headMhash)

	reply := docSyncReply{
		Results: []docSyncItem{
			{
				DocID: "docA",
				Heads: [][]byte{headCid.Bytes()},
			},
		},
		Sender: "12D3KooWRustPeer",
	}
	replyBytes, err := cbor.Marshal(reply)
	if err != nil {
		fmt.Fprintln(os.Stderr, "reply encode:", err)
		os.Exit(1)
	}

	// Wrap reply in an internalResponse the way Go's pubsub_rpc does at
	// rpc.go:381-389 (From left empty — receiver fills from validated source).
	envelope := internalResponse{
		ID:   requestID.String(),
		From: nil,
		Data: replyBytes,
		Err:  "",
	}
	envNode := bindnode.Wrap(&envelope, resType)
	envBytes, err := ipld.Encode(envNode.Representation(), dagcbor.Encode)
	if err != nil {
		fmt.Fprintln(os.Stderr, "envelope encode:", err)
		os.Exit(1)
	}

	fmt.Println("---- end-to-end replay fixture ----")
	fmt.Printf("request.doc_ids:      %v\n", req.DocIDs)
	fmt.Printf("request_bytes   (%d): %s\n", len(reqBytes), hex.EncodeToString(reqBytes))
	fmt.Printf("request_id:           %s\n", requestID.String())
	fmt.Printf("inner_reply     (%d): %s\n", len(replyBytes), hex.EncodeToString(replyBytes))
	fmt.Printf("envelope_bytes  (%d): %s\n", len(envBytes), hex.EncodeToString(envBytes))
}
