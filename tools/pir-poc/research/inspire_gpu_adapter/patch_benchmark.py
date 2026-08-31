#!/usr/bin/env python3
"""Instrument the pinned inspire-gpu benchmark without changing its protocol.

The upstream executable already measures warm online latency and small batches.
This checked patch adds phase boundaries that are needed for a cold-client and
cold-server comparison, and fills the database with the same deterministic
120-byte logical records as the Defra Dense/GPU-DPF adapter.
"""

from pathlib import Path
import sys


def replace_once(source: str, old: str, new: str, label: str) -> str:
    count = source.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one source match, found {count}")
    return source.replace(old, new, 1)


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: patch_benchmark.py PATH/TO/benches/bench_e2e.cu")

    path = Path(sys.argv[1])
    source = path.read_text()

    source = replace_once(
        source,
        """static double ms(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}
""",
        """static double ms(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

static uint32_t defra_mix32(uint32_t value) {
    value ^= value >> 16;
    value *= 0x7feb352du;
    value ^= value >> 15;
    value *= 0x846ca68bu;
    return value ^ (value >> 16);
}

static void defra_record(size_t ordinal, uint8_t out[120]) {
    for (size_t limb = 0; limb < 8; limb++) {
        uint32_t base = static_cast<uint32_t>(ordinal) ^
                        (0x9e3779b9u * static_cast<uint32_t>(limb + 1));
        uint32_t words[4] = {
            defra_mix32(base ^ 0xa5a5a5a5u),
            defra_mix32(base ^ 0x3c6ef372u),
            defra_mix32(base ^ 0xdaa66d2bu),
            defra_mix32(base ^ 0x78dde6e4u),
        };
        size_t bytes = limb == 7 ? 8 : 16;
        std::memcpy(out + limb * 16, words, bytes);
    }
}

static void defra_pack_15(const uint8_t in[120], uint16_t out[64]) {
    uint64_t bits = 0;
    unsigned available = 0;
    size_t input = 0;
    for (size_t slot = 0; slot < 64; slot++) {
        while (available < 15) {
            bits |= static_cast<uint64_t>(in[input++]) << available;
            available += 8;
        }
        out[slot] = static_cast<uint16_t>(bits & 0x7fffu);
        bits >>= 15;
        available -= 15;
    }
}
""",
        "common record helpers",
    )

    source = replace_once(
        source,
        """    // Slot-native DB: db_rows*db_cols values in [0,P), row-major. Fast
    // deterministic fill (an LCG) so 16 GB generation isn't the bottleneck.
    std::vector<uint16_t> db((size_t)pp.db_rows * pp.db_cols);
    uint64_t st = 0x9e3779b97f4a7c15ull;
    for (auto& s : db) { st = st * 6364136223846793005ull + 1; s = (uint16_t)((st >> 33) % P); }

    auto t0 = Clock::now();
    auto data = gpu_preprocess(pp, db.data());
    GpuServerConfig scfg;
""",
        """    // Exact logical corpus used by the Defra Dense/GPU-DPF adapter.
    // InsPIRe stores each 120-byte record as 64 consecutive 15-bit slots.
    auto materialize_start = Clock::now();
    std::vector<uint16_t> db((size_t)pp.db_rows * pp.db_cols, 0);
    uint8_t record[120];
    uint16_t slots[64];
    for (size_t idx = 0; idx < N_entries; idx++) {
        defra_record(idx, record);
        defra_pack_15(record, slots);
        size_t row = idx % pp.db_rows;
        size_t col = (idx / pp.db_rows) * cpe;
        std::memcpy(&db[row * pp.db_cols + col], slots, sizeof(slots));
    }
    printf("  materialize: %.2f ms host common-corpus encoding\\n",
           ms(materialize_start, Clock::now()));

    auto preprocess_start = Clock::now();
    auto data = gpu_preprocess(pp, db.data());
    cudaDeviceSynchronize();
    printf("  preprocess: %.2f ms GPU snapshot preprocessing\\n",
           ms(preprocess_start, Clock::now()));
    GpuServerConfig scfg;
""",
        "common database and preprocess boundary",
    )

    source = replace_once(
        source,
        """    auto* ctx = gpu_setup_server(pp, data, scfg);
    cudaDeviceSynchronize();
    printf("  setup: %.1f s (preprocess + server)\\n", ms(t0, Clock::now()) / 1000.0);

    // Correctness sanity on a few indices.
""",
        """    auto setup_start = Clock::now();
    auto* ctx = gpu_setup_server(pp, data, scfg);
    cudaDeviceSynchronize();
    printf("  server-context: %.2f ms after preprocessing\\n",
           ms(setup_start, Clock::now()));

    // First online request: no warmup and no retained client hint.  Keep
    // client generation, server answer, and client extraction separate.
    auto cold_client_start = Clock::now();
    auto [cold_qst, cold_qry] = query(pp, 17 % N_entries);
    double cold_client_ms = ms(cold_client_start, Clock::now());
    auto cold_server_start = Clock::now();
    auto cold_resp = gpu_answer(ctx, cold_qry);
    cudaDeviceSynchronize();
    double cold_server_ms = ms(cold_server_start, Clock::now());
    auto cold_extract_start = Clock::now();
    auto cold_slots = extract(pp, cold_qst, cold_resp);
    double cold_extract_ms = ms(cold_extract_start, Clock::now());
    size_t cold_row = (17 % N_entries) % pp.db_rows;
    size_t cold_col = ((17 % N_entries) / pp.db_rows) * cpe;
    bool cold_ok = cold_slots.size() == cpe;
    for (size_t k = 0; cold_ok && k < cpe; k++)
        cold_ok = cold_slots[k] == db[cold_row * pp.db_cols + cold_col + k];
    printf("  cold-online: client-query=%.2f ms server=%.2f ms "
           "client-extract=%.2f ms correctness=%s\\n",
           cold_client_ms, cold_server_ms, cold_extract_ms,
           cold_ok ? "true" : "false");

    // Correctness sanity on a few indices.
""",
        "cold online boundary",
    )

    source = replace_once(
        source,
        """int main(int argc, char** argv) {
    printf("=== inspire-gpu end-to-end benchmark (RTX 5090) ===\\n");
""",
        """int main(int argc, char** argv) {
    int device = 0;
    cudaDeviceProp prop{};
    cudaGetDevice(&device);
    cudaGetDeviceProperties(&prop, device);
    printf("=== inspire-gpu end-to-end benchmark (%s, cc %d.%d) ===\\n",
           prop.name, prop.major, prop.minor);
""",
        "dynamic GPU heading",
    )

    path.write_text(source)


if __name__ == "__main__":
    main()
