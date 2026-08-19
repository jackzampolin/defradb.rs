// Common-corpus adapter for the pinned Henzinger-Ragavan reference artifact.
//
// The runner copies this file below the upstream module.  It deliberately uses
// the artifact's exported Database, PickParams, EncodeDatabase, Query, Answer,
// and Recover operations without modifying the cryptographic implementation.
package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	fdpir "github.com/ahenzinger/finite-diffs-pir/pir"
	"github.com/zeebo/blake3"
)

const (
	schema           = "defra-finite-differences-artifact-v1"
	artifactRevision = "4574a4f8c52eeda165e110cbb64f834397d7c049"
)

type manifest struct {
	Schema          string `json:"schema"`
	PageCount       int    `json:"page_count"`
	PageBytes       int    `json:"page_bytes"`
	QueryIndex      int    `json:"query_index"`
	ExpectedPageHex string `json:"expected_page_hex"`
	CorpusBLAKE3    string `json:"corpus_blake3"`
}

type distribution struct {
	Samples []float64 `json:"samples_ms"`
	P50MS   float64   `json:"p50_ms"`
	P95MS   float64   `json:"p95_ms"`
}

type metric struct {
	Value  uint64 `json:"value"`
	Unit   string `json:"unit"`
	Status string `json:"status"`
	Note   string `json:"note"`
}

type report struct {
	Schema   string `json:"schema"`
	Artifact struct {
		Repository string `json:"repository"`
		Revision   string `json:"revision"`
		Runtime    string `json:"runtime"`
		Scope      string `json:"implementation_scope"`
	} `json:"artifact"`
	Workload struct {
		CorpusSchema   string `json:"corpus_schema"`
		CorpusBLAKE3   string `json:"corpus_blake3"`
		CorpusVerified bool   `json:"corpus_blake3_verified"`
		Records        int    `json:"records"`
		RecordBytes    int    `json:"record_bytes"`
		RawBytes       int    `json:"raw_bytes"`
		QueryIndex     int    `json:"query_index"`
		Mapping        string `json:"mapping"`
		Correct        bool   `json:"correct"`
		CorrectTrials  int    `json:"correct_trials"`
	} `json:"workload"`
	Parameters struct {
		Theta                 float64 `json:"theta"`
		VariablesM            int     `json:"variables_m"`
		DegreeD               int     `json:"degree_D"`
		RecordCapacity        int     `json:"record_capacity"`
		CloudRadius           int     `json:"cloud_radius"`
		CloudRecordsPerServer int     `json:"cloud_records_per_server"`
	} `json:"parameters"`
	Security struct {
		Privacy          string `json:"privacy"`
		Servers          int    `json:"server_count_s"`
		Threshold        int    `json:"collusion_threshold_t"`
		CollusionFailure string `json:"collusion_failure"`
		ManyServerNote   string `json:"many_server_warning"`
	} `json:"security"`
	Setup struct {
		BuildTimeMS float64 `json:"build_time_ms"`
		PeakRSS     metric  `json:"peak_process_rss"`
	} `json:"setup"`
	Online struct {
		ClientQuery                  distribution `json:"client_query"`
		Server0Answer                distribution `json:"server_0_answer"`
		Server1Answer                distribution `json:"server_1_answer"`
		AggregateServerAnswer        distribution `json:"aggregate_server_answer"`
		ClientRecover                distribution `json:"client_recover"`
		UploadArtifactRepresentation metric       `json:"upload_artifact_representation"`
		UploadLogicalMinimum         metric       `json:"upload_logical_minimum"`
		Download                     metric       `json:"download"`
		LogicalRecordReads           metric       `json:"logical_record_reads"`
		LogicalReadBytes             metric       `json:"logical_read_bytes"`
	} `json:"online"`
	Storage struct {
		PaperServerStorage       metric `json:"paper_server_storage"`
		AggregateDeployedStorage metric `json:"aggregate_deployed_storage"`
	} `json:"storage"`
	Warnings []string `json:"warnings"`
}

func deterministic(value uint64, unit, note string) metric {
	return metric{Value: value, Unit: unit, Status: "deterministic", Note: note}
}

func measured(value uint64, unit, note string) metric {
	return metric{Value: value, Unit: unit, Status: "measured", Note: note}
}

func cloud(p *fdpir.Params) []int {
	points := make([]int, 0)
	for weight := 0; weight <= p.D/2; weight++ {
		positions := fdpir.FirstCombination(weight)
		for {
			point := 0
			for _, position := range positions {
				point |= 1 << position
			}
			points = append(points, point)
			if !fdpir.NextCombination(positions, p.M) {
				break
			}
		}
	}
	sort.Ints(points)
	return points
}

func summarize(samples []float64) distribution {
	ordered := append([]float64(nil), samples...)
	sort.Float64s(ordered)
	quantile := func(q float64) float64 {
		if len(ordered) == 0 {
			return 0
		}
		index := int(float64(len(ordered)-1)*q + 0.5)
		return ordered[index]
	}
	return distribution{Samples: samples, P50MS: quantile(0.50), P95MS: quantile(0.95)}
}

func readRSS() uint64 {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kib, err := strconv.ParseUint(fields[1], 10, 64)
		if err == nil {
			return kib * 1024
		}
	}
	return 0
}

func samplePeakRSS(stop <-chan struct{}) <-chan uint64 {
	result := make(chan uint64, 1)
	go func() {
		peak := readRSS()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if rss := readRSS(); rss > peak {
					peak = rss
				}
			case <-stop:
				if rss := readRSS(); rss > peak {
					peak = rss
				}
				result <- peak
				return
			}
		}
	}()
	return result
}

func main() {
	corpusPath := flag.String("corpus", "", "path to pages.bin")
	manifestPath := flag.String("manifest", "", "path to manifest.json")
	outputPath := flag.String("output", "", "path to result JSON")
	samples := flag.Int("samples", 7, "measured online trials")
	theta := flag.Float64("theta", 0.5, "artifact parameter-selection theta")
	flag.Parse()
	if *corpusPath == "" || *manifestPath == "" || *outputPath == "" || *samples < 1 {
		panic("--corpus, --manifest, --output, and positive --samples are required")
	}

	manifestBytes, err := os.ReadFile(*manifestPath)
	if err != nil {
		panic(err)
	}
	var manifest manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		panic(err)
	}
	if manifest.Schema != "defra-pir-raw-page-corpus-v1" {
		panic("unexpected corpus schema")
	}
	corpus, err := os.ReadFile(*corpusPath)
	if err != nil {
		panic(err)
	}
	if len(corpus) != manifest.PageCount*manifest.PageBytes {
		panic("corpus dimensions do not match the manifest")
	}
	corpusDigest := blake3.Sum256(corpus)
	if !strings.EqualFold(hex.EncodeToString(corpusDigest[:]), manifest.CorpusBLAKE3) {
		panic("corpus BLAKE3 does not match the manifest")
	}
	fmt.Fprintf(os.Stderr, "phase=corpus_hash status=verified blake3=%x\n", corpusDigest)
	expected, err := hex.DecodeString(manifest.ExpectedPageHex)
	if err != nil || len(expected) != manifest.PageBytes {
		panic("invalid expected page in manifest")
	}

	params := fdpir.PickParams(manifest.PageCount, manifest.PageBytes, *theta)
	if params.M >= 63 {
		panic("artifact query representation supports fewer than 63 variables")
	}
	points := cloud(params)
	database := &fdpir.Database{
		Num_records: manifest.PageCount,
		Record_len:  manifest.PageBytes,
		Data:        corpus,
	}

	stopRSS := make(chan struct{})
	peakRSS := samplePeakRSS(stopRSS)
	fmt.Fprintf(os.Stderr, "phase=encode status=start records=%d record_bytes=%d m=%d D=%d\n", manifest.PageCount, manifest.PageBytes, params.M, params.D)
	buildStart := time.Now()
	encoded := fdpir.EncodeDatabase(database, params)
	buildTime := time.Since(buildStart)
	close(stopRSS)
	peak := <-peakRSS
	fmt.Fprintf(os.Stderr, "phase=encode status=complete elapsed=%s peak_sampled_rss_bytes=%d\n", buildTime, peak)
	if encoded.Bytelen() != (1<<params.M)*manifest.PageBytes {
		panic("unexpected encoded database size")
	}

	queryTimes := make([]float64, 0, *samples)
	server0Times := make([]float64, 0, *samples)
	server1Times := make([]float64, 0, *samples)
	aggregateTimes := make([]float64, 0, *samples)
	recoverTimes := make([]float64, 0, *samples)
	correct := 0
	for trial := 0; trial < *samples; trial++ {
		fmt.Fprintf(os.Stderr, "phase=online status=start trial=%d\n", trial)
		start := time.Now()
		state, query0, query1 := fdpir.Query(manifest.QueryIndex, params)
		queryTimes = append(queryTimes, float64(time.Since(start))/float64(time.Millisecond))

		var answer0, answer1 []byte
		if trial%2 == 0 {
			start = time.Now()
			answer0 = fdpir.Answer(encoded, points, query0)
			elapsed0 := time.Since(start)
			start = time.Now()
			answer1 = fdpir.Answer(encoded, points, query1)
			elapsed1 := time.Since(start)
			server0Times = append(server0Times, float64(elapsed0)/float64(time.Millisecond))
			server1Times = append(server1Times, float64(elapsed1)/float64(time.Millisecond))
			aggregateTimes = append(aggregateTimes, float64(elapsed0+elapsed1)/float64(time.Millisecond))
		} else {
			start = time.Now()
			answer1 = fdpir.Answer(encoded, points, query1)
			elapsed1 := time.Since(start)
			start = time.Now()
			answer0 = fdpir.Answer(encoded, points, query0)
			elapsed0 := time.Since(start)
			server0Times = append(server0Times, float64(elapsed0)/float64(time.Millisecond))
			server1Times = append(server1Times, float64(elapsed1)/float64(time.Millisecond))
			aggregateTimes = append(aggregateTimes, float64(elapsed0+elapsed1)/float64(time.Millisecond))
		}

		start = time.Now()
		recovered := fdpir.Recover(params, points, state, answer0, answer1)
		recoverTimes = append(recoverTimes, float64(time.Since(start))/float64(time.Millisecond))
		if !bytes.Equal(recovered, expected) {
			panic(fmt.Sprintf("trial %d recovered the wrong page", trial))
		}
		correct++
		fmt.Fprintf(os.Stderr, "phase=online status=complete trial=%d aggregate_server_ms=%.6f\n", trial, aggregateTimes[len(aggregateTimes)-1])
	}

	answerBytes := uint64(len(points) * manifest.PageBytes)
	encodedBytes := uint64(encoded.Bytelen())
	logicalQueryBytes := uint64((params.M + 7) / 8)
	var result report
	result.Schema = schema
	result.Artifact.Repository = "https://github.com/ahenzinger/finite-diffs-pir"
	result.Artifact.Revision = artifactRevision
	result.Artifact.Runtime = runtime.Version()
	result.Artifact.Scope = "official two-server F_2 implementation invoked through exported APIs"
	result.Workload.CorpusSchema = manifest.Schema
	result.Workload.CorpusBLAKE3 = manifest.CorpusBLAKE3
	result.Workload.CorpusVerified = true
	result.Workload.Records = manifest.PageCount
	result.Workload.RecordBytes = manifest.PageBytes
	result.Workload.RawBytes = len(corpus)
	result.Workload.QueryIndex = manifest.QueryIndex
	result.Workload.Mapping = "one exact populated Defra page is one artifact record"
	result.Workload.Correct = correct == *samples
	result.Workload.CorrectTrials = correct
	result.Parameters.Theta = *theta
	result.Parameters.VariablesM = params.M
	result.Parameters.DegreeD = params.D
	result.Parameters.RecordCapacity = fdpir.Binomial(params.M, params.D)
	result.Parameters.CloudRadius = params.D / 2
	result.Parameters.CloudRecordsPerServer = len(points)
	result.Security.Privacy = "perfect query privacy against either one semi-honest server"
	result.Security.Servers = 2
	result.Security.Threshold = 1
	result.Security.CollusionFailure = "the two queries reveal the encoded target when the servers collude"
	result.Security.ManyServerNote = "Theorem 5.3 is a different q-ary construction and is not implemented by this artifact."
	result.Setup.BuildTimeMS = float64(buildTime) / float64(time.Millisecond)
	result.Setup.PeakRSS = measured(peak, "bytes", "1 ms /proc/self/status samples from adapter start through EncodeDatabase completion")
	result.Online.ClientQuery = summarize(queryTimes)
	result.Online.Server0Answer = summarize(server0Times)
	result.Online.Server1Answer = summarize(server1Times)
	result.Online.AggregateServerAnswer = summarize(aggregateTimes)
	result.Online.ClientRecover = summarize(recoverTimes)
	result.Online.UploadArtifactRepresentation = deterministic(16, "bytes", "two 64-bit query integers; no framing or transport")
	result.Online.UploadLogicalMinimum = deterministic(2*logicalQueryBytes, "bytes", "two packed m-bit points; not serialized by the prototype")
	result.Online.Download = deterministic(2*answerBytes, "bytes", "sum of both server answers")
	result.Online.LogicalRecordReads = deterministic(2*uint64(len(points)), "records", "sum of both servers as required by paper Definition 2.5")
	result.Online.LogicalReadBytes = deterministic(2*answerBytes, "bytes", "generic C lookup copies every byte of every selected record")
	result.Storage.PaperServerStorage = deterministic(encodedBytes, "bytes", "Definition 2.4 counts DB' once; also the per-replica size")
	result.Storage.AggregateDeployedStorage = deterministic(2*encodedBytes, "bytes", "two independently operated replicas")
	result.Warnings = []string{
		"Direct in-process calls do not measure network, framing, TLS, filesystem, or energy.",
		"The two server timings are summed; parallel wall time is deliberately not labeled aggregate work.",
		"The official parameter picker uses one padded polynomial table, not Corollary 3.3's multi-chunk construction.",
		"The official encoder is allocation-heavy; the runner must enforce its RSS guard.",
	}

	encodedReport, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		panic(err)
	}
	encodedReport = append(encodedReport, '\n')
	if err := os.WriteFile(*outputPath, encodedReport, 0o644); err != nil {
		panic(err)
	}
	os.Stdout.Write(encodedReport)
}
