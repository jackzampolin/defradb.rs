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
	"encoding/hex"
	"fmt"
	"os"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	_ "github.com/ipld/go-ipld-prime/schema"
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
}
