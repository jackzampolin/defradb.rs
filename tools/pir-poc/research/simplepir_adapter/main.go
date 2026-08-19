// Command defra-simplepir-adapter runs the pinned upstream SimplePIR or
// DoublePIR implementation on Defra's layout-neutral 96-byte page corpus.
//
// The reproduction script copies this file into a scratch checkout of the
// official repository. No upstream source file is patched or vendored.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"time"

	upstream "github.com/ahenzinger/simplepir/pir"
)

const (
	artifactRevision = "e9020b03bf2872c75b8954e749e32408b5db87ed"
	logQ             = uint64(32)
	securityN        = uint64(1 << 10)
	laneBytes        = 4
)

type corpusManifest struct {
	Schema          string `json:"schema"`
	DocumentCount   int    `json:"document_count"`
	DistinctTags    int    `json:"distinct_tag_count"`
	PageCount       int    `json:"page_count"`
	PageBytes       int    `json:"page_bytes"`
	QueryIndex      int    `json:"query_index"`
	ExpectedPageHex string `json:"expected_page_hex"`
	CorpusBLAKE3    string `json:"corpus_blake3"`
}

type metric struct {
	Value    any    `json:"value"`
	Evidence string `json:"evidence"`
	Note     string `json:"note"`
}

type runResult struct {
	Protocol                 string
	Params                   upstream.Params
	Info                     upstream.DBinfo
	PageCount                uint64
	PageBytes                uint64
	LaneCount                uint64
	PaddedPagesPerLane       uint64
	LogicalEntryCapacity     uint64
	LogicalCorpusBytes       uint64
	AlignmentPaddingBytes    uint64
	UnsquishedMatrixBytes    uint64
	PackedDatabaseBytes      uint64
	ServerStateBytes         uint64
	HintBytes                uint64
	CompressedSeedBytes      uint64
	DecompressedSharedBytes  uint64
	CorpusTransformMS        float64
	DatabaseBuildMS          float64
	SharedInitMS             float64
	HintSetupMS              float64
	ClientSetupMS            float64
	ClientQueryP50MS         float64
	ServerOnlineP50MS        float64
	ClientRecoverP50MS       float64
	UploadBytes              uint64
	DownloadBytes            uint64
	HeapAllocAfterSetupBytes uint64
	HeapSysAfterSetupBytes   uint64
}

func main() {
	corpusPath := flag.String("corpus", "", "path to pages.bin")
	manifestPath := flag.String("manifest", "", "path to manifest.json")
	protocolName := flag.String("protocol", "simple", "simple or double")
	samples := flag.Int("samples", 3, "number of correctness-checked online samples")
	outputPath := flag.String("output", "", "aggregate-work JSON output path")
	flag.Parse()
	if *corpusPath == "" || *manifestPath == "" || *outputPath == "" || *samples < 1 {
		fatalf("--corpus, --manifest, --output, and --samples >= 1 are required")
	}

	manifestBytes, err := os.ReadFile(*manifestPath)
	check(err)
	var manifest corpusManifest
	check(json.Unmarshal(manifestBytes, &manifest))
	if manifest.Schema != "defra-pir-raw-page-corpus-v1" {
		fatalf("unsupported corpus schema %q", manifest.Schema)
	}
	raw, err := os.ReadFile(*corpusPath)
	check(err)
	if manifest.PageBytes != 96 || manifest.PageBytes%laneBytes != 0 {
		fatalf("adapter requires 96-byte pages split into 32-bit lanes; got %d", manifest.PageBytes)
	}
	if len(raw) != manifest.PageCount*manifest.PageBytes {
		fatalf("corpus length %d does not match manifest %d x %d", len(raw), manifest.PageCount, manifest.PageBytes)
	}
	if manifest.QueryIndex < 0 || manifest.QueryIndex >= manifest.PageCount {
		fatalf("query index is outside the corpus")
	}
	expected, err := hex.DecodeString(manifest.ExpectedPageHex)
	check(err)
	selected := raw[manifest.QueryIndex*manifest.PageBytes : (manifest.QueryIndex+1)*manifest.PageBytes]
	if !bytes.Equal(selected, expected) {
		fatalf("manifest expected page does not match pages.bin")
	}

	var protocol upstream.PIR
	switch *protocolName {
	case "simple":
		protocol = &upstream.SimplePIR{}
	case "double":
		protocol = &upstream.DoublePIR{}
	default:
		fatalf("unknown protocol %q", *protocolName)
	}

	result := run(protocol, raw, manifest, *samples)
	report := aggregateWorkReport(result, manifest, *samples)
	encoded, err := json.MarshalIndent(report, "", "  ")
	check(err)
	encoded = append(encoded, '\n')
	check(os.WriteFile(*outputPath, encoded, 0o644))
	fmt.Printf("wrote %s: %s server p50 %.3f ms, upload %d B, download %d B, hint %d B\n",
		*outputPath, result.Protocol, result.ServerOnlineP50MS, result.UploadBytes,
		result.DownloadBytes, result.HintBytes)
}

func run(protocol upstream.PIR, raw []byte, manifest corpusManifest, samples int) runResult {
	lanes := uint64(manifest.PageBytes / laneBytes)
	pages := uint64(manifest.PageCount)
	p, info, stride, capacity := alignedParams(protocol, pages, lanes)

	transformStart := time.Now()
	values := make([]uint64, capacity)
	for page := uint64(0); page < pages; page++ {
		row := raw[page*uint64(manifest.PageBytes) : (page+1)*uint64(manifest.PageBytes)]
		for lane := uint64(0); lane < lanes; lane++ {
			values[lane*stride+page] = uint64(binary.LittleEndian.Uint32(row[lane*laneBytes : (lane+1)*laneBytes]))
		}
	}
	transformMS := elapsedMS(transformStart)

	databaseStart := time.Now()
	database := upstream.MakeDB(capacity, laneBytes*8, &p, values)
	databaseBuildMS := elapsedMS(databaseStart)
	values = nil
	runtime.GC()

	sharedStart := time.Now()
	serverShared, compressed := protocol.InitCompressed(database.Info, p)
	sharedInitMS := elapsedMS(sharedStart)
	clientSetupStart := time.Now()
	clientShared := protocol.DecompressState(database.Info, p, compressed)
	clientSetupMS := elapsedMS(clientSetupStart)

	setupStart := time.Now()
	serverState, hint := protocol.Setup(database, serverShared, p)
	hintSetupMS := elapsedMS(setupStart)
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)

	queryTimes := make([]float64, 0, samples)
	answerTimes := make([]float64, 0, samples)
	recoverTimes := make([]float64, 0, samples)
	var upload, download uint64
	for sample := 0; sample < samples; sample++ {
		queryStart := time.Now()
		queries := upstream.MsgSlice{}
		clientStates := make([]upstream.State, 0, lanes)
		for lane := uint64(0); lane < lanes; lane++ {
			index := uint64(manifest.QueryIndex) + lane*stride
			clientState, query := protocol.Query(index, clientShared, p, database.Info)
			clientStates = append(clientStates, clientState)
			queries.Data = append(queries.Data, query)
		}
		queryTimes = append(queryTimes, elapsedMS(queryStart))

		answerStart := time.Now()
		answer := protocol.Answer(database, queries, serverState, serverShared, p)
		answerTimes = append(answerTimes, elapsedMS(answerStart))

		recoverStart := time.Now()
		recovered := make([]byte, manifest.PageBytes)
		for lane := uint64(0); lane < lanes; lane++ {
			index := uint64(manifest.QueryIndex) + lane*stride
			value := protocol.Recover(index, lane, hint, queries.Data[lane], answer,
				clientShared, clientStates[lane], p, database.Info)
			binary.LittleEndian.PutUint32(recovered[lane*laneBytes:(lane+1)*laneBytes], uint32(value))
		}
		recoverTimes = append(recoverTimes, elapsedMS(recoverStart))
		if !bytes.Equal(recovered, raw[manifest.QueryIndex*manifest.PageBytes:(manifest.QueryIndex+1)*manifest.PageBytes]) {
			fatalf("%s sample %d failed 96-byte page reconstruction", protocol.Name(), sample)
		}
		upload = queries.Size() * (logQ / 8)
		download = answer.Size() * (logQ / 8)
	}

	return runResult{
		Protocol:                 protocol.Name(),
		Params:                   p,
		Info:                     info,
		PageCount:                pages,
		PageBytes:                uint64(manifest.PageBytes),
		LaneCount:                lanes,
		PaddedPagesPerLane:       stride,
		LogicalEntryCapacity:     capacity,
		LogicalCorpusBytes:       uint64(len(raw)),
		AlignmentPaddingBytes:    (capacity - pages*lanes) * laneBytes,
		UnsquishedMatrixBytes:    p.L * p.M * (logQ / 8),
		PackedDatabaseBytes:      database.Data.Size() * (logQ / 8),
		ServerStateBytes:         stateBytes(serverState),
		HintBytes:                hint.Size() * (logQ / 8),
		CompressedSeedBytes:      16,
		DecompressedSharedBytes:  stateBytes(clientShared),
		CorpusTransformMS:        transformMS,
		DatabaseBuildMS:          databaseBuildMS,
		SharedInitMS:             sharedInitMS,
		HintSetupMS:              hintSetupMS,
		ClientSetupMS:            clientSetupMS,
		ClientQueryP50MS:         median(queryTimes),
		ServerOnlineP50MS:        median(answerTimes),
		ClientRecoverP50MS:       median(recoverTimes),
		UploadBytes:              upload,
		DownloadBytes:            download,
		HeapAllocAfterSetupBytes: memory.HeapAlloc,
		HeapSysAfterSetupBytes:   memory.HeapSys,
	}
}

func alignedParams(protocol upstream.PIR, pages, lanes uint64) (upstream.Params, upstream.DBinfo, uint64, uint64) {
	p := protocol.PickParams(pages*lanes, laneBytes*8, securityN, logQ)
	for attempt := 0; attempt < 12; attempt++ {
		probe := upstream.SetupDB(pages*lanes, laneBytes*8, &p)
		alignment := probe.Info.Ne * lanes
		alignedL := ((p.L + alignment - 1) / alignment) * alignment
		if alignedL != p.L {
			p = protocol.PickParamsGivenDimensions(alignedL, p.M, securityN, logQ)
			continue
		}
		capacity := (p.L / probe.Info.Ne) * p.M
		stride := capacity / lanes
		if stride < pages || capacity%lanes != 0 {
			p = protocol.PickParamsGivenDimensions(p.L+alignment, p.M, securityN, logQ)
			continue
		}
		info := upstream.SetupDB(capacity, laneBytes*8, &p).Info
		if info.Ne != probe.Info.Ne || p.L%(info.Ne*lanes) != 0 {
			continue
		}
		return p, info, stride, capacity
	}
	fatalf("could not find official parameters whose matrix rows split evenly across %d page lanes", lanes)
	panic("unreachable")
}

func aggregateWorkReport(result runResult, manifest corpusManifest, samples int) map[string]any {
	serverStorage := result.PackedDatabaseBytes + result.ServerStateBytes
	clientPersistent := result.HintBytes + result.CompressedSeedBytes
	clientOnline := result.ClientQueryP50MS + result.ClientRecoverP50MS
	return map[string]any{
		"schema":   "pir-aggregate-work-v1",
		"protocol": result.Protocol,
		"artifact": map[string]any{
			"repository":             "https://github.com/ahenzinger/simplepir",
			"revision":               artifactRevision,
			"upstream_modifications": "none; this out-of-tree adapter is copied into a scratch checkout as a new command package",
			"adapter_qualification":  "96-byte pages are split into 24 little-endian 32-bit lanes. The official batch API answers all lanes in one aggregate pass over disjoint row bands. Parameters use the official PickParamsGivenDimensions API solely to make those bands exact.",
			"fuse_multi_hot":         "not supported by the official point-query API: its arithmetic sum is not the bytewise XOR required by Fuse-4; four independent cell PIRs would be a different workload",
		},
		"comparison_scope": map[string]any{
			"workload":         fmt.Sprintf("%d populated immutable 96-byte Defra tag pages", manifest.PageCount),
			"result":           "one exact 96-byte useful page",
			"public_partition": "global snapshot",
			"leakage":          map[string]any{"class": "exact_query_privacy"},
		},
		"security": map[string]any{
			"privacy":             "single-server computational query privacy under the upstream LWE parameters",
			"server_count":        1,
			"collusion_tolerance": 0,
			"required_answers":    1,
			"assumptions":         "LWE; semi-honest research prototype; compressed public matrix seed",
			"availability":        "the one server is mandatory",
			"integrity":           "the reconstructed page fingerprint can detect many wrong pages; no malicious-server PIR proof",
		},
		"global_build": map[string]any{
			"unit":                     "immutable snapshot",
			"aggregate_server_time_ms": m(nil, "not_measured", "component phases were timed separately; no synthetic sum is presented as a jointly timed build"),
			"database_encoding_ms":     m(result.DatabaseBuildMS, "measured", "upstream MakeDB only"),
			"shared_matrix_init_ms":    m(result.SharedInitMS, "measured", "upstream InitCompressed on this run"),
			"hint_setup_ms":            m(result.HintSetupMS, "measured", "upstream Setup on the populated corpus"),
			"client_download_bytes":    m(result.HintBytes, "deterministic", "DB-specific offline hint; compressed public-matrix seed is reported in client state"),
			"peak_server_ram_bytes":    m(nil, "not_measured", "Go heap snapshots are supplied as diagnostics but are not a phase peak"),
		},
		"per_client_setup": map[string]any{
			"unit":                  "client snapshot initialization",
			"client_time_ms":        m(result.ClientSetupMS, "measured", "decompress the seeded public matrix once"),
			"client_download_bytes": m(result.HintBytes+result.CompressedSeedBytes, "deterministic", "one public hint plus one 16-byte seed; cacheable across clients for the snapshot"),
			"server_time_ms":        m(0.0, "deterministic", "no client-specific server preprocessing"),
		},
		"online": map[string]any{
			"unit": "one exact 96-byte useful page",
			"per_server": []any{map[string]any{
				"server_index":              0,
				"server_time_p50_ms":        m(result.ServerOnlineP50MS, "measured", fmt.Sprintf("median of %d correctness-checked samples", samples)),
				"logical_selected_bytes":    m(result.LogicalCorpusBytes, "estimated", "the batched lane layout performs one aggregate pass over the logical page bytes; arithmetic representation is larger"),
				"physical_or_scanned_bytes": m(result.PackedDatabaseBytes, "estimated", "size of the packed upstream matrix, not a hardware counter"),
				"scans":                     m(1, "deterministic", "24 official batch bands together cover the packed database once"),
			}},
			"aggregate_server_time_p50_ms":        m(result.ServerOnlineP50MS, "measured", "one server, so aggregate equals per-server time"),
			"max_server_time_p50_ms":              m(result.ServerOnlineP50MS, "measured", "one server"),
			"aggregate_logical_selected_bytes":    m(result.LogicalCorpusBytes, "estimated", "logical page corpus before LWE encoding and batch-alignment padding"),
			"aggregate_physical_or_scanned_bytes": m(result.PackedDatabaseBytes, "estimated", "packed upstream matrix size; hardware counters were not collected"),
			"server_scans":                        m(1, "deterministic", "one aggregate scan split across 24 batch bands"),
			"network_rounds":                      m(1, "deterministic", "online query and answer after cached setup"),
			"useful_result_bytes":                 m(result.PageBytes, "deterministic", "complete encoded Defra page"),
		},
		"maintenance": map[string]any{
			"unit":                     "new immutable snapshot",
			"aggregate_server_time_ms": m(nil, "not_measured", "a changed snapshot repeats the separately reported build phases; no joint maintenance run was timed"),
		},
		"client": map[string]any{
			"online_cpu_p50_ms":         m(clientOnline, "measured", "query generation plus reconstruction; excludes network"),
			"query_cpu_p50_ms":          m(result.ClientQueryP50MS, "measured", "24 official batch query messages for one 96-byte page"),
			"recover_cpu_p50_ms":        m(result.ClientRecoverP50MS, "measured", "reconstruct and join 24 32-bit lanes"),
			"peak_transient_ram_bytes":  m(nil, "not_measured", "requires a process-isolated peak/RSS collector"),
			"persistent_state_bytes":    m(clientPersistent, "deterministic", "DB hint plus compressed public-matrix seed; decompressed matrix can be regenerated"),
			"decompressed_shared_bytes": m(result.DecompressedSharedBytes, "deterministic", "in-memory public matrices held by this adapter during online queries"),
			"upload_bytes":              m(result.UploadBytes, "deterministic", "all 24 lane query messages at 32 bits per matrix element"),
			"download_bytes":            m(result.DownloadBytes, "deterministic", "one batched upstream answer"),
		},
		"persisted_storage": map[string]any{
			"server_bytes_per_server":     m(serverStorage, "deterministic", "packed in-memory database plus protocol server state; excludes allocator overhead and public setup matrix after Setup"),
			"aggregate_server_bytes":      m(serverStorage, "deterministic", "single server"),
			"client_bytes":                m(clientPersistent, "deterministic", "cacheable hint plus 16-byte seed"),
			"packed_database_bytes":       m(result.PackedDatabaseBytes, "deterministic", "upstream squished matrix allocation"),
			"unsquished_matrix_bytes":     m(result.UnsquishedMatrixBytes, "deterministic", "p.L x p.M 32-bit matrix allocation before the upstream 3-way squish"),
			"protocol_server_state_bytes": m(result.ServerStateBytes, "deterministic", "upstream server State matrices"),
		},
		"amortization": map[string]any{
			"global_build":                     "all queries served by one immutable snapshot",
			"per_client_setup":                 "all queries made while the client retains the public hint/seed",
			"maintenance":                      "new snapshot rebuild",
			"assumed_global_queries":           nil,
			"assumed_queries_per_client_setup": nil,
			"note":                             "phase costs are kept separate; no favorable query-count denominator is assumed",
		},
		"corpus": map[string]any{
			"schema":                        manifest.Schema,
			"blake3":                        manifest.CorpusBLAKE3,
			"page_count":                    result.PageCount,
			"page_bytes":                    result.PageBytes,
			"logical_bytes":                 result.LogicalCorpusBytes,
			"lane_count":                    result.LaneCount,
			"lane_bits":                     laneBytes * 8,
			"padded_pages_per_lane":         result.PaddedPagesPerLane,
			"logical_entry_capacity":        result.LogicalEntryCapacity,
			"batch_alignment_padding_bytes": result.AlignmentPaddingBytes,
			"unsquished_matrix_rows":        result.Params.L,
			"unsquished_matrix_columns":     result.Params.M,
			"plaintext_modulus":             result.Params.P,
			"transform_ms":                  m(result.CorpusTransformMS, "measured", "raw page bytes to lane-major uint64 input array"),
		},
		"runner_diagnostics": map[string]any{
			"go_version":                   runtime.Version(),
			"goos":                         runtime.GOOS,
			"goarch":                       runtime.GOARCH,
			"heap_alloc_after_setup_bytes": result.HeapAllocAfterSetupBytes,
			"heap_sys_after_setup_bytes":   result.HeapSysAfterSetupBytes,
			"warning":                      "heap figures are point-in-time diagnostics, not peak measurements",
		},
		"hardware_counters": map[string]any{
			"adapter":        "none",
			"physical_bytes": "not measured; packed matrix size is labeled estimated scanned bytes",
			"cpu_energy":     "not measured",
			"dram_energy":    "not measured",
		},
	}
}

func m(value any, evidence, note string) metric {
	return metric{Value: value, Evidence: evidence, Note: note}
}

func stateBytes(state upstream.State) uint64 {
	var elements uint64
	for _, matrix := range state.Data {
		elements += matrix.Size()
	}
	return elements * (logQ / 8)
}

func median(values []float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	middle := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[middle-1] + sorted[middle]) / 2
	}
	return sorted[middle]
}

func elapsedMS(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000
}

func check(err error) {
	if err != nil {
		fatalf("%v", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "defra-simplepir-adapter: "+format+"\n", args...)
	os.Exit(1)
}
