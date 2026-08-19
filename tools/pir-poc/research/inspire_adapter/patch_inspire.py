#!/usr/bin/env python3
"""Apply the Defra common-corpus adapter to the exact InsPIRe Zenodo source.

The paper artifact keeps its complete protocol in `src/bin/inspire.rs` and has
no database-input API.  Keeping these checked replacements outside the
artifact makes the semantic delta auditable without vendoring its 1,000-line
binary.  Every replacement must match exactly once.
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
        raise SystemExit("usage: patch_inspire.py PATH/TO/src/bin/inspire.rs")

    path = Path(sys.argv[1])
    source = path.read_text()

    source = replace_once(
        source,
        "use std::{marker::PhantomData, time::Instant};\n",
        "use std::{fs, marker::PhantomData, time::Instant};\n",
        "filesystem import",
    )

    source = replace_once(
        source,
        """pub fn run_simple_ypir_rgsw_on_params(
    params: Params,
    interpolate_degree: usize,
    trials: usize,
    online_only: bool
) -> Measurement {
""",
        """pub struct DefraCorpus {
    raw: Vec<u8>,
    page_bytes: usize,
    target_page: usize,
}

pub fn run_simple_ypir_rgsw_on_params(
    params: Params,
    interpolate_degree: usize,
    trials: usize,
    online_only: bool,
    defra_corpus: Option<&DefraCorpus>,
) -> Measurement {
""",
        "corpus input type and function argument",
    )

    source = replace_once(
        source,
        """    let mut pt_iter = std::iter::repeat_with(|| (T::sample() as u64 % params.pt_modulus) as T);
    let mut actual_db = Vec::with_capacity(db_cols*db_rows);
    for _ in 0..db_cols*db_rows {
        actual_db.push(pt_iter.next().unwrap());
    }
""",
        """    let mut pt_iter = std::iter::repeat_with(|| (T::sample() as u64 % params.pt_modulus) as T);
    let mut actual_db = vec![0u16; db_cols * db_rows];
    if let Some(corpus) = defra_corpus {
        assert!(corpus.page_bytes > 0);
        assert_eq!(corpus.raw.len() % corpus.page_bytes, 0);
        let page_count = corpus.raw.len() / corpus.page_bytes;
        assert!(corpus.target_page < page_count);

        // A query identifies (row, interpolation sub-column) and returns
        // c*gamma coefficients.  Keep pages inside those result blocks.
        // One byte per residue is exact for all bytes under p=65535.
        let values_per_block = c * gamma;
        let pages_per_block = values_per_block / corpus.page_bytes;
        assert!(pages_per_block > 0);
        let needed_blocks = page_count.div_ceil(pages_per_block);
        assert!(needed_blocks <= db_rows * interpolate_degree);

        for page in 0..page_count {
            let block = page / pages_per_block;
            let page_in_block = page % pages_per_block;
            let row = block / interpolate_degree;
            let target_sub_col = block % interpolate_degree;
            for byte_in_page in 0..corpus.page_bytes {
                let output_index = page_in_block * corpus.page_bytes + byte_in_page;
                let which_poly = output_index / gamma;
                let coefficient = output_index % gamma;
                let col = which_poly * interpolate_degree * gamma
                    + target_sub_col * gamma
                    + coefficient;
                actual_db[col * db_rows + row] =
                    corpus.raw[page * corpus.page_bytes + byte_in_page] as u16;
            }
        }
    } else {
        for value in &mut actual_db {
            *value = pt_iter.next().unwrap();
        }
    }
""",
        "database materialization",
    )

    source = replace_once(
        source,
        """        let target_idx: usize = rng.gen::<usize>() % (db_rows * db_cols);
        let target_row = target_idx / db_cols;
""",
        """        let target_idx: usize = if let Some(corpus) = defra_corpus {
            let pages_per_block = (c * gamma) / corpus.page_bytes;
            let block = corpus.target_page / pages_per_block;
            let row = block / interpolate_degree;
            let target_sub_col = block % interpolate_degree;
            row * db_cols + target_sub_col * gamma
        } else {
            rng.gen::<usize>() % (db_rows * db_cols)
        };
        let target_row = target_idx / db_cols;
""",
        "private block selection",
    )

    source = replace_once(
        source,
        """        if online_only {
            println!("Warning!")
        } else {
            assert_eq!(final_result, sub_corr_result);
        }

        measurement.online.upload_keys = pub_params_size;
""",
        """        if online_only {
            println!("Warning!")
        } else {
            assert_eq!(final_result, sub_corr_result);
        }
        if let Some(corpus) = defra_corpus {
            let pages_per_block = (c * gamma) / corpus.page_bytes;
            let page_in_block = corpus.target_page % pages_per_block;
            let start = page_in_block * corpus.page_bytes;
            let recovered = final_result[start..start + corpus.page_bytes]
                .iter().map(|value| *value as u8).collect::<Vec<_>>();
            let expected_start = corpus.target_page * corpus.page_bytes;
            assert_eq!(recovered, corpus.raw[expected_start..expected_start + corpus.page_bytes]);
        }

        measurement.online.upload_keys = pub_params_size;
""",
        "page correctness gate",
    )

    source = replace_once(
        source,
        """    #[clap(long, short, action)]
    online_only: bool,

}
""",
        """    #[clap(long, short, action)]
    online_only: bool,

    /// Exact Defra page corpus. One byte maps to one p=65535 residue.
    #[clap(long)]
    defra_corpus: Option<String>,

    /// Page selected for every correctness-checked artifact trial.
    #[clap(long, default_value_t = 17)]
    defra_target_page: usize,

    /// Deterministic mapping report, separate from artifact measurements.
    #[clap(long)]
    defra_mapping_json: Option<String>,

}
""",
        "CLI fields",
    )

    source = replace_once(
        source,
        """        verbose,
        online_only,
    } = args;
""",
        """        verbose,
        online_only,
        defra_corpus,
        defra_target_page,
        defra_mapping_json,
    } = args;
""",
        "CLI destructuring",
    )

    source = replace_once(
        source,
        """    let trials = trials.unwrap_or(1);
    let label = label.unwrap_or("".to_string());

    println!(
""",
        """    let trials = trials.unwrap_or(1);
    let label = label.unwrap_or("".to_string());

    let page_bytes = item_size_bits.div_ceil(8);
    let defra_corpus = defra_corpus.map(|path| DefraCorpus {
        raw: fs::read(path).expect("read Defra corpus"),
        page_bytes,
        target_page: defra_target_page,
    });
    if let Some(corpus) = defra_corpus.as_ref() {
        assert_eq!(item_size_bits % 8, 0, "Defra pages must be byte aligned");
        assert_eq!(corpus.raw.len(), num_items * page_bytes, "Defra corpus geometry");
    }
    // One arbitrary byte per p=65535 coefficient is an exact 16-bit physical
    // representation. Keep raw useful geometry separate in the report.
    let parameter_item_size_bits = if defra_corpus.is_some() {
        item_size_bits * 2
    } else {
        item_size_bits
    };

    println!(
""",
        "corpus loading and physical parameter width",
    )

    source = replace_once(
        source,
        """    let given_d0 = interpolate_degree == 0;
    let (params, interpolate_degree, (resized_db_first_dim, resized_db_second_dim, resized_item_size_bits)) = if given_d0 {
""",
        """    let given_d0 = interpolate_degree == 0;
    if defra_corpus.is_some() {
        assert!(given_d0, "Defra corpus adapter requires the p=65535 dim0 parameter path");
        assert!(!small_params, "Defra corpus adapter does not use the p=65 small-parameter path");
    }
    let (params, interpolate_degree, (resized_db_first_dim, resized_db_second_dim, resized_item_size_bits)) = if given_d0 {
""",
        "Defra parameter-path gate",
    )

    source = replace_once(
        source,
        """            params_rgswpir_given_input_size_and_dim0_small(num_items, item_size_bits, dim0)
        } else {
            params_rgswpir_given_input_size_and_dim0(num_items, item_size_bits, dim0)
        }
    } else {
        params_rgswpir_given_interpolate_degree(num_items, interpolate_degree, item_size_bits)
    };
""",
        """            params_rgswpir_given_input_size_and_dim0_small(num_items, parameter_item_size_bits, dim0)
        } else {
            params_rgswpir_given_input_size_and_dim0(num_items, parameter_item_size_bits, dim0)
        }
    } else {
        params_rgswpir_given_interpolate_degree(num_items, interpolate_degree, parameter_item_size_bits)
    };
""",
        "parameterized physical width",
    )

    source = replace_once(
        source,
        (
            """    let mut measurement = run_simple_ypir_rgsw_on_params(params, interpolate_degree, trials, online_only);
"""
            + "        \n"
            + """    measurement.specs.resized_db_first_dim = resized_db_first_dim;
"""
        ),
        """    let gamma = params.poly_len;
    let db_rows = 1 << (params.db_dim_1 + params.poly_len_log2);
    let db_cols = params.instances * params.poly_len;
    let c = (db_cols / gamma) / interpolate_degree;
    let values_per_block = c * gamma;
    let pages_per_block = values_per_block / page_bytes;
    if defra_corpus.is_some() {
        assert!(pages_per_block > 0);
        assert!(num_items.div_ceil(pages_per_block) <= db_rows * interpolate_degree);
    }

    let mut measurement = run_simple_ypir_rgsw_on_params(
        params,
        interpolate_degree,
        trials,
        online_only,
        defra_corpus.as_ref(),
    );

    measurement.specs.resized_db_first_dim = resized_db_first_dim;
""",
        "adapter invocation and block geometry",
    )

    source = replace_once(
        source,
        """    measurement.specs.interpolate_degree = interpolate_degree;
    measurement.specs.label = label;

    if let Some(out_report_json) = out_report_json {
""",
        """    measurement.specs.interpolate_degree = interpolate_degree;
    measurement.specs.label = if defra_corpus.is_some() {
        format!("{};defra-common-corpus", label)
    } else {
        label
    };

    if let Some(path) = defra_mapping_json {
        let mapping = serde_json::json!({
            "schema": "defra-inspire-corpus-mapping-v1",
            "page_count": num_items,
            "page_bytes": page_bytes,
            "useful_corpus_bytes": num_items * page_bytes,
            "physical_bits_per_page_for_parameters": parameter_item_size_bits,
            "plaintext_modulus": measurement.specs.pt_modulus,
            "one_byte_per_plaintext_coefficient": true,
            "values_per_private_result_block": values_per_block,
            "pages_per_private_result_block": pages_per_block,
            "useful_bytes_per_private_result_block": pages_per_block * page_bytes,
            "private_result_block_capacity_bytes": values_per_block,
            "target_page": defra_target_page,
            "correctness": "selected page checked byte-for-byte after artifact decryption",
            "qualification": "physical encoding and block retrieval differ from one 96-byte record; raw semantics are recovered locally without server-visible selection"
        });
        fs::write(path, serde_json::to_vec_pretty(&mapping).unwrap())
            .expect("write Defra mapping report");
    }

    if let Some(out_report_json) = out_report_json {
""",
        "mapping report",
    )

    path.write_text(source)


if __name__ == "__main__":
    main()
