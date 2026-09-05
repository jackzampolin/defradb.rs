"""Select the author's existing AVX2/scalar branch on the local Ryzen CPU."""
from pathlib import Path
import sys
path=Path(sys.argv[1])/'cmake/CompilerFlags.cmake'
source=path.read_text()
old='    enable_avx512(${target})'
if old in source:path.write_text(source.replace(old,'    # Cold pilot: native host has AVX2, no AVX512; use existing fallback.'))
elif 'Cold pilot:' not in source:raise ValueError('unexpected source')
path=Path(sys.argv[1])/'src/params.cpp';source=path.read_text()
if 'Cold pilot scalar RNS' not in source:
    start=source.index('                for (size_t k = 0; k < PRIME_COUNT; k += 8)')
    end=source.index('                }\n\n            }',start)+len('                }')
    original=source[start:end]
    replacement='#ifdef __AVX512F__\n'+original+'''\n#else
                // Cold pilot scalar RNS: identical low-64-bit product/addition.
                for (size_t k = 0; k < PRIME_COUNT; ++k) {
                    rns_result[row][k] += rns_hint[row][col][k] * rns_offsets[col][k];
                }
#endif'''
    path.write_text(source[:start]+replacement+source[end:])
path=Path(sys.argv[1])/'include/utils.h';source=path.read_text()
if 'COLD_PHASE_CPU_MS' not in source:
    source='#include <ctime>\n'+source
    source=source.replace('    auto start = high_resolution_clock::now();','    auto cold_cpu_start = std::clock();\n    auto start = high_resolution_clock::now();')
    source=source.replace('    auto stop = high_resolution_clock::now();','    auto stop = high_resolution_clock::now();\n    std::cout << "COLD_PHASE_CPU_MS " << label << ": " << 1000.0*(std::clock()-cold_cpu_start)/CLOCKS_PER_SEC << std::endl;')
    path.write_text(source)
