#!/usr/bin/env python3
"""Adapt Poulpy's pinned end-to-end example to the Defra 120-byte corpus."""

from pathlib import Path
import sys


def once(source: str, old: str, new: str, label: str) -> str:
    count = source.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one source match, found {count}")
    return source.replace(old, new, 1)


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: patch_benchmark.py PATH/TO/defra_bench.rs")
    path = Path(sys.argv[1])
    source = path.read_text()
    source = once(
        source,
        "use poulpy_cpu_avx512::FFT64Avx512;",
        "use poulpy_cpu_avx::FFT64Avx;",
        "AVX backend",
    )
    source = once(
        source,
        "config::{Collapse, Config, DefaultPirConfig32B, DefaultPirParameters32B},",
        "config::{Collapse, Config, DefaultPirParameters32B},",
        "config imports",
    )
    source = once(
        source,
        "payload::Payload,",
        "payload::{P65536, Payload},",
        "payload import",
    )
    source = once(source, "type BE = FFT64Avx512;", "type BE = FFT64Avx;", "backend alias")
    source = once(source, "const REPEATS: usize = 10;", "const REPEATS: usize = 5;", "repeats")
    source = once(
        source,
        """    println!("preset                       : {}", preset.name());
    match preset.resolve() {
        DefaultPirConfig32B::Interpolation(p) => run(p.config, p.layout, ITEM_INDEX, batch),
        DefaultPirConfig32B::Recursion(p) => run(p.config, p.layout, ITEM_INDEX, batch),
    }
""",
        """    println!("schema                       : defradb-poulpy-cpu-avx2-v1");
    println!("preset                       : {}", preset.name());
    let config = Config::<P65536<[u8; 128]>>::with_collapse(preset.collapse());
    let layout = DatabaseLayout::<P65536<[u8; 128]>>::new(preset.rows(), preset.cols());
    run(config, layout, ITEM_INDEX, batch)
""",
        "Defra payload main",
    )
    source = source.replace("[u8; 32]", "[u8; 128]")
    source = source.replace("[0u8; 32]", "[0u8; 128]")
    source = once(source, "1usize << 22", "1usize << 20", "fill chunk")
    source = once(
        source,
        """fn fill_payloads(out: &mut [[u8; 128]], first_index: usize) {
    for (i, payload) in out.iter_mut().enumerate() {
        let index = (first_index + i) as u64;
        for word in 0..4u64 {
            let mut x = (index * 4 + word).wrapping_add(0x9e3779b97f4a7c15);
            x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
            x ^= x >> 31;
            payload[word as usize * 8..][..8].copy_from_slice(&x.to_le_bytes());
        }
    }
}
""",
        """fn mix32(mut value: u32) -> u32 {
    value ^= value >> 16;
    value = value.wrapping_mul(0x7feb352d);
    value ^= value >> 15;
    value = value.wrapping_mul(0x846ca68b);
    value ^ (value >> 16)
}

fn fill_payloads(out: &mut [[u8; 128]], first_index: usize) {
    for (i, payload) in out.iter_mut().enumerate() {
        *payload = [0u8; 128];
        let ordinal = (first_index + i) as u32;
        for limb in 0..8u32 {
            let base = ordinal ^ 0x9e3779b9u32.wrapping_mul(limb + 1);
            let words = [
                mix32(base ^ 0xa5a5a5a5),
                mix32(base ^ 0x3c6ef372),
                mix32(base ^ 0xdaa66d2b),
                mix32(base ^ 0x78dde6e4),
            ];
            let count = if limb == 7 { 2 } else { 4 };
            for (word, value) in words.into_iter().take(count).enumerate() {
                let offset = limb as usize * 16 + word * 4;
                payload[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            }
        }
    }
}
""",
        "common corpus fill",
    )
    source = source.replace("num_payloads * 32", "num_payloads * 128")
    source = source.replace("{} x 32 B", "{} x 128 B")
    path.write_text(source)


if __name__ == "__main__":
    main()
