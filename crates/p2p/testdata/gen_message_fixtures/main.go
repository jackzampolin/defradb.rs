// Generate byte-exact fxamacker/cbor v2 fixtures for the DocSync and
// BranchableSync pubsub_rpc payload structs, matching the encoder used by
// sourcenetwork/defradb:
//
//	cbor.Marshal(req) // github.com/fxamacker/cbor/v2, default opts
//
// See `defradb/internal/db/p2p/sync_doc.go:112, :303` and
// `sync_branchable_col.go:107, :271`. Default opts emit struct fields in
// declaration order with definite-length maps and honor the `json:` tags.
//
// Run: `go run main.go` — prints the hex-encoded CBOR bytes for several
// fixtures so the Rust side can bake them in as parity constants.
package main

import (
	"encoding/hex"
	"fmt"
	"os"

	"github.com/fxamacker/cbor/v2"
)

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

type syncBranchableCollectionRequest struct {
	CollectionID string `json:"collectionID"`
}

type syncBranchableCollectionReply struct {
	CollectionID string   `json:"collectionID"`
	Heads        [][]byte `json:"heads"`
	Sender       string   `json:"sender"`
}

type metaData struct {
	Version    string
	MessageID  string
	SenderID   string
	Pubkey     []byte
	Signature  []byte `cbor:",omitempty"`
	ErrMessage string `cbor:",omitempty"`
}

type pushLogRequest struct {
	metaData
	DocID        string
	CID          []byte
	CollectionID string
	Creator      string
	Block        []byte
}

type fixture struct {
	label string
	value any
}

func main() {
	fixtures := []fixture{
		{
			label: "push_log_request",
			value: pushLogRequest{
				metaData: metaData{
					Version:   "/defradb/0.0.1",
					MessageID: "msg-go",
					SenderID:  "peer-go",
					Pubkey:    []byte{0x01, 0x02},
					Signature: []byte{0x03, 0x04},
				},
				DocID:        "bafy-doc",
				CID:          []byte{0x01, 0x71, 0xaa},
				CollectionID: "bafy-collection",
				Creator:      "did:key:zGo",
				Block:        []byte{0xa1, 0x61, 0x78, 0x01},
			},
		},
		{
			label: "doc_sync_request_two_ids",
			value: docSyncRequest{DocIDs: []string{"docA", "docB"}},
		},
		{
			label: "doc_sync_request_empty",
			value: docSyncRequest{DocIDs: []string{}},
		},
		{
			label: "doc_sync_item",
			value: docSyncItem{
				DocID: "bafy-doc-id",
				Heads: [][]byte{
					{0x01, 0x02, 0x03},
					{0xff, 0xee, 0xdd, 0xcc},
				},
			},
		},
		{
			label: "doc_sync_reply",
			value: docSyncReply{
				Results: []docSyncItem{
					{
						DocID: "bafy-1",
						Heads: [][]byte{{0xde, 0xad, 0xbe, 0xef}},
					},
					{
						DocID: "bafy-2",
						Heads: [][]byte{{0x00, 0x11}},
					},
				},
				Sender: "12D3KooWPeer",
			},
		},
		{
			label: "doc_sync_reply_empty",
			value: docSyncReply{Results: nil, Sender: "peer"},
		},
		{
			label: "branchable_sync_request",
			value: syncBranchableCollectionRequest{CollectionID: "bafy-collection"},
		},
		{
			label: "branchable_sync_reply",
			value: syncBranchableCollectionReply{
				CollectionID: "bafy-collection",
				Heads: [][]byte{
					{0xaa, 0xbb, 0xcc},
					{0x99, 0x88},
				},
				Sender: "12D3KooWPeer",
			},
		},
		{
			label: "branchable_sync_reply_empty_heads",
			value: syncBranchableCollectionReply{
				CollectionID: "bafy-collection",
				Heads:        nil,
				Sender:       "peer",
			},
		},
	}

	for _, f := range fixtures {
		b, err := cbor.Marshal(f.value)
		if err != nil {
			fmt.Fprintf(os.Stderr, "encode %s: %v\n", f.label, err)
			os.Exit(1)
		}
		fmt.Printf("fixture: %s\n", f.label)
		fmt.Printf("  value: %+v\n", f.value)
		fmt.Printf("  cbor  (%d bytes): %s\n\n", len(b), hex.EncodeToString(b))
	}
}
