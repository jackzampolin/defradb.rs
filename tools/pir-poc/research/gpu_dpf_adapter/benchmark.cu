// Copyright 2026 DefraDB PIR POC contributors.
// SPDX-License-Identifier: Apache-2.0
//
// Research-only adapter around the pinned facebookresearch/GPU-DPF artifact.
// It compares two-server DPF-PIR with a bit-packed two-server Dense XOR
// control on the same GPU-resident, fixed-width table.  The DPF construction,
// ChaCha12 PRF, and fused expansion/reduction kernel come from upstream.

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#ifndef DEFRA_LIMBS
#define DEFRA_LIMBS 8
#endif

#define MM DEFRA_LIMBS
#include "dpf_gpu/dpf/dpf_hybrid.cu"

namespace {

constexpr int kThreads = 128;
constexpr int kUsefulSnapshotBytes = 120;
constexpr int kUsefulLiveBytes = 16;
constexpr int kVisibleCandidates = 100;
constexpr int kVisibleTargetSlot = 37;
constexpr int kPrfMethod = CHACHA20;
constexpr const char* kUpstreamCommit =
    "ce23a06af884ee54300b5bc5fd5350e445f10b0b";

struct Options {
  int entries = 1 << 20;
  int batch = 1;
  int samples = 7;
  int min_sample_ms = 50;
  bool live = false;
  bool dpf_first = false;
};

struct Timings {
  double client_ms = 0.0;
  bool first_online_measured = false;
  double server_context_ms = 0.0;
  double first_h2d_aggregate_ms = 0.0;
  double first_server0_ms = 0.0;
  double first_server1_ms = 0.0;
  double first_aggregate_server_ms = 0.0;
  double first_parallel_server_ms = 0.0;
  double first_d2h_aggregate_ms = 0.0;
  double h2d_p50_ms = 0.0;
  double server0_p50_ms = 0.0;
  double server1_p50_ms = 0.0;
  double aggregate_p50_ms = 0.0;
  double wall_p50_ms = 0.0;
  double d2h_p50_ms = 0.0;
  double mean_gpu_power_watts = 0.0;
  double peak_gpu_power_watts = 0.0;
  double approximate_gpu_joules_per_query = 0.0;
  size_t power_samples = 0;
  int repetitions = 1;
};

struct VisibleTimings {
  double registration_client_ms = 0.0;
  double server_p50_ms = 0.0;
  double client_filter_p50_ms = 0.0;
  int repetitions = 1;
};

volatile uint64_t visible_sink = 0;

class PowerSampler {
 public:
  PowerSampler() {
    library_ = dlopen("libnvidia-ml.so.1", RTLD_LAZY);
    if (library_ == nullptr) {
      return;
    }
    init_ = reinterpret_cast<Init>(dlsym(library_, "nvmlInit_v2"));
    get_handle_ = reinterpret_cast<GetHandle>(
        dlsym(library_, "nvmlDeviceGetHandleByIndex_v2"));
    get_power_ = reinterpret_cast<GetPower>(
        dlsym(library_, "nvmlDeviceGetPowerUsage"));
    shutdown_ = reinterpret_cast<Shutdown>(dlsym(library_, "nvmlShutdown"));
    if (init_ == nullptr || get_handle_ == nullptr || get_power_ == nullptr ||
        shutdown_ == nullptr || init_() != 0 || get_handle_(0, &device_) != 0) {
      dlclose(library_);
      library_ = nullptr;
    }
  }

  ~PowerSampler() {
    stop();
    if (library_ != nullptr) {
      shutdown_();
      dlclose(library_);
    }
  }

  void start() {
    if (library_ == nullptr) {
      return;
    }
    running_ = true;
    started_ = std::chrono::steady_clock::now();
    thread_ = std::thread([this]() {
      while (running_) {
        unsigned int milliwatts = 0;
        if (get_power_(device_, &milliwatts) == 0) {
          watts_.push_back(milliwatts / 1000.0);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    });
  }

  void stop() {
    if (!running_) {
      return;
    }
    running_ = false;
    if (thread_.joinable()) {
      thread_.join();
    }
    elapsed_ = std::chrono::steady_clock::now() - started_;
  }

  void apply(Timings* timing, size_t logical_queries) const {
    timing->power_samples = watts_.size();
    if (watts_.empty() || logical_queries == 0) {
      return;
    }
    double total = 0.0;
    for (double watts : watts_) {
      total += watts;
      timing->peak_gpu_power_watts =
          std::max(timing->peak_gpu_power_watts, watts);
    }
    timing->mean_gpu_power_watts = total / watts_.size();
    timing->approximate_gpu_joules_per_query =
        timing->mean_gpu_power_watts *
        std::chrono::duration<double>(elapsed_).count() / logical_queries;
  }

 private:
  using Init = int (*)();
  using GetHandle = int (*)(unsigned int, void**);
  using GetPower = int (*)(void*, unsigned int*);
  using Shutdown = int (*)();

  void* library_ = nullptr;
  void* device_ = nullptr;
  Init init_ = nullptr;
  GetHandle get_handle_ = nullptr;
  GetPower get_power_ = nullptr;
  Shutdown shutdown_ = nullptr;
  std::atomic<bool> running_{false};
  std::thread thread_;
  std::vector<double> watts_;
  std::chrono::steady_clock::time_point started_;
  std::chrono::steady_clock::duration elapsed_{};
};

void check_cuda(cudaError_t result, const char* expression, const char* file,
                int line) {
  if (result != cudaSuccess) {
    std::fprintf(stderr, "CUDA failure at %s:%d for %s: %s\n", file, line,
                 expression, cudaGetErrorString(result));
    std::exit(2);
  }
}

#define DEFRA_CUDA(expression) \
  check_cuda((expression), #expression, __FILE__, __LINE__)

Options parse_options(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto value = [&](const char* name) -> const char* {
      if (++i >= argc) {
        throw std::runtime_error(std::string("missing value for ") + name);
      }
      return argv[i];
    };
    if (arg == "--entries") {
      options.entries = std::stoi(value("--entries"));
    } else if (arg == "--batch") {
      options.batch = std::stoi(value("--batch"));
    } else if (arg == "--samples") {
      options.samples = std::stoi(value("--samples"));
    } else if (arg == "--min-sample-ms") {
      options.min_sample_ms = std::stoi(value("--min-sample-ms"));
    } else if (arg == "--live") {
      options.live = true;
    } else if (arg == "--protocol-order") {
      const std::string order = value("--protocol-order");
      if (order == "dense-first") {
        options.dpf_first = false;
      } else if (order == "dpf-first") {
        options.dpf_first = true;
      } else {
        throw std::runtime_error(
            "--protocol-order must be dense-first or dpf-first");
      }
    } else {
      throw std::runtime_error("unknown argument: " + arg);
    }
  }
  if (options.entries < 256 ||
      (options.entries & (options.entries - 1)) != 0) {
    throw std::runtime_error("--entries must be a power of two >= 256");
  }
  if (options.batch < 1 || options.batch > 4096) {
    throw std::runtime_error("--batch must be between 1 and 4096");
  }
  if (options.samples < 3 || options.samples > 101) {
    throw std::runtime_error("--samples must be between 3 and 101");
  }
  if (options.live && DEFRA_LIMBS != 1) {
    throw std::runtime_error("--live requires a DEFRA_LIMBS=1 binary");
  }
  if (!options.live && DEFRA_LIMBS != 8) {
    throw std::runtime_error("snapshot mode requires DEFRA_LIMBS=8");
  }
  return options;
}

double milliseconds(std::chrono::steady_clock::duration elapsed) {
  return std::chrono::duration<double, std::milli>(elapsed).count();
}

double median(std::vector<double> values) {
  std::sort(values.begin(), values.end());
  return values[values.size() / 2];
}

__host__ __device__ uint32_t reverse_bits(uint32_t value) {
  value = ((value >> 1) & 0x55555555u) | ((value & 0x55555555u) << 1);
  value = ((value >> 2) & 0x33333333u) | ((value & 0x33333333u) << 2);
  value = ((value >> 4) & 0x0f0f0f0fu) | ((value & 0x0f0f0f0fu) << 4);
  value = ((value >> 8) & 0x00ff00ffu) | ((value & 0x00ff00ffu) << 8);
  return (value >> 16) | (value << 16);
}

int log2_entries(int entries) {
  int depth = 0;
  while ((1 << depth) < entries) {
    ++depth;
  }
  return depth;
}

__host__ __device__ uint32_t mix32(uint32_t value) {
  value ^= value >> 16;
  value *= 0x7feb352du;
  value ^= value >> 15;
  value *= 0x846ca68bu;
  return value ^ (value >> 16);
}

__host__ __device__ uint4 record_limb(uint32_t logical_ordinal, int limb,
                                      bool live) {
  uint4 result;
  if (live) {
    // A histogram entry.  The target bucket is deliberately non-zero.
    result.x = logical_ordinal % 7 == 0 ? 3u : 0u;
    result.y = 0;
    result.z = 0;
    result.w = 0;
    return result;
  }
  const uint32_t base = logical_ordinal ^ (0x9e3779b9u * (limb + 1));
  result.x = mix32(base ^ 0xa5a5a5a5u);
  result.y = mix32(base ^ 0x3c6ef372u);
  result.z = mix32(base ^ 0xdaa66d2bu);
  result.w = mix32(base ^ 0x78dde6e4u);
  if (limb == DEFRA_LIMBS - 1) {
    // A 120-byte useful record occupies seven complete uint4 limbs plus the
    // low eight bytes of the final limb.  Both GPU protocols still process a
    // 128-byte physical row; wire accounting truncates it back to 120 bytes.
    result.z = 0;
    result.w = 0;
  }
  return result;
}

__global__ void initialize_table_kernel(uint4* table, int entries, int depth,
                                        bool live) {
  const int physical = blockIdx.x * blockDim.x + threadIdx.x;
  if (physical >= entries) {
    return;
  }
  const uint32_t logical = reverse_bits(static_cast<uint32_t>(physical)) >>
                           (32 - depth);
  for (int limb = 0; limb < DEFRA_LIMBS; ++limb) {
    table[static_cast<size_t>(limb) * entries + physical] =
        record_limb(logical, limb, live);
  }
}

__host__ __device__ uint4 xor4(uint4 left, uint4 right) {
  left.x ^= right.x;
  left.y ^= right.y;
  left.z ^= right.z;
  left.w ^= right.w;
  return left;
}

__global__ void dense_partial_kernel(const uint4* table,
                                     const uint32_t* selectors,
                                     uint4* partial, int entries,
                                     int blocks_per_query, int batch) {
  const int query = blockIdx.x / blocks_per_query;
  const int query_block = blockIdx.x % blocks_per_query;
  if (query >= batch) {
    return;
  }
  uint4 accum[DEFRA_LIMBS] = {};
  const size_t selector_base =
      static_cast<size_t>(query) * static_cast<size_t>(entries / 32);
  for (int row = query_block * blockDim.x + threadIdx.x; row < entries;
       row += blocks_per_query * blockDim.x) {
    const uint32_t selected =
        (selectors[selector_base + static_cast<size_t>(row / 32)] >>
         (row & 31)) &
        1u;
    const uint32_t mask = 0u - selected;
    for (int limb = 0; limb < DEFRA_LIMBS; ++limb) {
      uint4 value = table[static_cast<size_t>(limb) * entries + row];
      value.x &= mask;
      value.y &= mask;
      value.z &= mask;
      value.w &= mask;
      accum[limb] = xor4(accum[limb], value);
    }
  }

  __shared__ uint4 shared[kThreads][DEFRA_LIMBS];
  for (int limb = 0; limb < DEFRA_LIMBS; ++limb) {
    shared[threadIdx.x][limb] = accum[limb];
  }
  __syncthreads();
  for (int stride = blockDim.x / 2; stride > 0; stride >>= 1) {
    if (threadIdx.x < stride) {
      for (int limb = 0; limb < DEFRA_LIMBS; ++limb) {
        shared[threadIdx.x][limb] =
            xor4(shared[threadIdx.x][limb], shared[threadIdx.x + stride][limb]);
      }
    }
    __syncthreads();
  }
  if (threadIdx.x == 0) {
    const size_t destination =
        (static_cast<size_t>(query) * blocks_per_query + query_block) *
        DEFRA_LIMBS;
    for (int limb = 0; limb < DEFRA_LIMBS; ++limb) {
      partial[destination + limb] = shared[0][limb];
    }
  }
}

__global__ void dense_finish_kernel(const uint4* partial, uint4* output,
                                    int blocks_per_query, int batch) {
  const int query = blockIdx.x;
  if (query >= batch) {
    return;
  }
  uint4 accum[DEFRA_LIMBS] = {};
  for (int block = threadIdx.x; block < blocks_per_query;
       block += blockDim.x) {
    const size_t source =
        (static_cast<size_t>(query) * blocks_per_query + block) * DEFRA_LIMBS;
    for (int limb = 0; limb < DEFRA_LIMBS; ++limb) {
      accum[limb] = xor4(accum[limb], partial[source + limb]);
    }
  }
  __shared__ uint4 shared[kThreads][DEFRA_LIMBS];
  for (int limb = 0; limb < DEFRA_LIMBS; ++limb) {
    shared[threadIdx.x][limb] = accum[limb];
  }
  __syncthreads();
  for (int stride = blockDim.x / 2; stride > 0; stride >>= 1) {
    if (threadIdx.x < stride) {
      for (int limb = 0; limb < DEFRA_LIMBS; ++limb) {
        shared[threadIdx.x][limb] =
            xor4(shared[threadIdx.x][limb], shared[threadIdx.x + stride][limb]);
      }
    }
    __syncthreads();
  }
  if (threadIdx.x == 0) {
    for (int limb = 0; limb < DEFRA_LIMBS; ++limb) {
      output[static_cast<size_t>(query) * DEFRA_LIMBS + limb] =
          shared[0][limb];
    }
  }
}

__global__ void presence_partial_kernel(const uint32_t* presence,
                                        const uint32_t* selectors,
                                        uint32_t* partial, int words,
                                        int blocks_per_query, int batch) {
  const int query = blockIdx.x / blocks_per_query;
  const int query_block = blockIdx.x % blocks_per_query;
  if (query >= batch) {
    return;
  }
  const size_t selector_base = static_cast<size_t>(query) * words;
  uint32_t parity = 0;
  for (int word = query_block * blockDim.x + threadIdx.x; word < words;
       word += blocks_per_query * blockDim.x) {
    parity ^= static_cast<uint32_t>(
        __popc(selectors[selector_base + word] & presence[word]) & 1);
  }
  __shared__ uint32_t shared[kThreads];
  shared[threadIdx.x] = parity;
  __syncthreads();
  for (int stride = blockDim.x / 2; stride > 0; stride >>= 1) {
    if (threadIdx.x < stride) {
      shared[threadIdx.x] ^= shared[threadIdx.x + stride];
    }
    __syncthreads();
  }
  if (threadIdx.x == 0) {
    partial[static_cast<size_t>(query) * blocks_per_query + query_block] =
        shared[0];
  }
}

__global__ void presence_finish_kernel(const uint32_t* partial,
                                       uint32_t* output,
                                       int blocks_per_query, int batch) {
  const int query = blockIdx.x;
  if (query >= batch) {
    return;
  }
  uint32_t parity = 0;
  for (int block = threadIdx.x; block < blocks_per_query;
       block += blockDim.x) {
    parity ^= partial[static_cast<size_t>(query) * blocks_per_query + block];
  }
  __shared__ uint32_t shared[kThreads];
  shared[threadIdx.x] = parity;
  __syncthreads();
  for (int stride = blockDim.x / 2; stride > 0; stride >>= 1) {
    if (threadIdx.x < stride) {
      shared[threadIdx.x] ^= shared[threadIdx.x + stride];
    }
    __syncthreads();
  }
  if (threadIdx.x == 0) {
    output[query] = shared[0] & 1u;
  }
}

uint32_t next_random(uint64_t& state) {
  state ^= state >> 12;
  state ^= state << 25;
  state ^= state >> 27;
  return static_cast<uint32_t>(state * 0x2545f4914f6cdd1dULL);
}

std::vector<int> target_ordinals(const Options& options) {
  std::vector<int> targets(options.batch);
  for (int query = 0; query < options.batch; ++query) {
    int target = static_cast<int>((0x9e3779b97f4a7c15ULL * (query + 1) +
                                   0xd1b54a32d192ed03ULL) &
                                  static_cast<uint64_t>(options.entries - 1));
    if (options.live) {
      // Every fourth subscription observes a non-zero histogram bucket; the
      // rest prove the fixed miss path as part of correctness checking.
      target = query % 4 == 0 ? (query * 7) & (options.entries - 1)
                              : (query * 7 + 1) & (options.entries - 1);
    }
    targets[query] = target;
  }
  return targets;
}

void delete_codeword_tree(SeedsCodewords* root) {
  while (root != nullptr) {
    SeedsCodewords* next = root->sub;
    root->sub = nullptr;
    delete root;
    root = next;
  }
}

struct DpfKeyPair {
  std::vector<SeedsCodewordsFlatGPU> server0;
  std::vector<SeedsCodewordsFlatGPU> server1;
};

DpfKeyPair make_dpf_keys(const Options& options,
                         const std::vector<int>& targets,
                         double* elapsed_ms) {
  const auto started = std::chrono::steady_clock::now();
  DpfKeyPair result{std::vector<SeedsCodewordsFlatGPU>(options.batch),
                    std::vector<SeedsCodewordsFlatGPU>(options.batch)};
  for (int query = 0; query < options.batch; ++query) {
    std::mt19937 generator(0x5eed1234u + static_cast<uint32_t>(query));
    SeedsCodewords* tree = GenerateSeedsAndCodewordsLog(
        targets[query], static_cast<uint128_t>(1), options.entries, generator,
        kPrfMethod);
    SeedsCodewordsFlat flat0{};
    SeedsCodewordsFlat flat1{};
    FlattenCodewords(tree, 0, &flat0);
    FlattenCodewords(tree, 1, &flat1);
    result.server0[query] = SeedsCodewordsFlatGPUFromCPU(flat0);
    result.server1[query] = SeedsCodewordsFlatGPUFromCPU(flat1);
    delete_codeword_tree(tree);
  }
  *elapsed_ms = milliseconds(std::chrono::steady_clock::now() - started);
  return result;
}

struct DenseSelectorPair {
  std::vector<uint32_t> server0;
  std::vector<uint32_t> server1;
};

DenseSelectorPair make_dense_selectors(const Options& options,
                                       const std::vector<int>& targets,
                                       double* elapsed_ms) {
  const auto started = std::chrono::steady_clock::now();
  const size_t words_per_query = static_cast<size_t>(options.entries) / 32;
  DenseSelectorPair selectors{
      std::vector<uint32_t>(words_per_query * options.batch), {}};
  uint64_t state = 0x4d595df4d0f33173ULL;
  for (uint32_t& word : selectors.server0) {
    word = next_random(state);
  }
  selectors.server1 = selectors.server0;
  for (int query = 0; query < options.batch; ++query) {
    const uint32_t physical =
        reverse_bits(static_cast<uint32_t>(targets[query])) >>
        (32 - log2_entries(options.entries));
    selectors.server1[static_cast<size_t>(query) * words_per_query +
                      physical / 32] ^= 1u << (physical & 31);
  }
  *elapsed_ms = milliseconds(std::chrono::steady_clock::now() - started);
  return selectors;
}

float elapsed_event(cudaEvent_t start, cudaEvent_t stop) {
  float result = 0;
  DEFRA_CUDA(cudaEventElapsedTime(&result, start, stop));
  return result;
}

int calibrated_repetitions(double first_ms, int minimum_ms) {
  if (first_ms <= 0.0) {
    return 1;
  }
  return std::max(1, std::min(1000,
      static_cast<int>(std::ceil(minimum_ms / first_ms))));
}

void verify_dense(const Options& options, const std::vector<int>& targets,
                  const std::vector<uint4>& server0,
                  const std::vector<uint4>& server1) {
  for (int query = 0; query < options.batch; ++query) {
    for (int limb = 0; limb < DEFRA_LIMBS; ++limb) {
      const uint4 got = xor4(server0[static_cast<size_t>(query) * DEFRA_LIMBS + limb],
                             server1[static_cast<size_t>(query) * DEFRA_LIMBS + limb]);
      const uint4 expected = record_limb(targets[query], limb, options.live);
      if (std::memcmp(&got, &expected, sizeof(uint4)) != 0) {
        throw std::runtime_error("Dense XOR reconstruction failed");
      }
    }
  }
}

uint4 subtract128(uint4 left, uint4 right) {
  uint4 result{};
  uint64_t lhs = left.x;
  uint64_t rhs = right.x;
  uint64_t value = lhs - rhs;
  result.x = static_cast<uint32_t>(value);
  uint64_t borrow = lhs < rhs;
  lhs = left.y;
  rhs = static_cast<uint64_t>(right.y) + borrow;
  value = lhs - rhs;
  result.y = static_cast<uint32_t>(value);
  borrow = lhs < rhs;
  lhs = left.z;
  rhs = static_cast<uint64_t>(right.z) + borrow;
  value = lhs - rhs;
  result.z = static_cast<uint32_t>(value);
  borrow = lhs < rhs;
  lhs = left.w;
  rhs = static_cast<uint64_t>(right.w) + borrow;
  result.w = static_cast<uint32_t>(lhs - rhs);
  return result;
}

void verify_dpf(const Options& options, const std::vector<int>& targets,
                const std::vector<uint4>& server0,
                const std::vector<uint4>& server1) {
  for (int query = 0; query < options.batch; ++query) {
    for (int limb = 0; limb < DEFRA_LIMBS; ++limb) {
      // The upstream construction guarantees server-0 minus server-1 equals
      // the requested point function.
      const uint4 got = subtract128(
          server0[query + static_cast<size_t>(options.batch) * limb],
          server1[query + static_cast<size_t>(options.batch) * limb]);
      const uint4 expected = record_limb(targets[query], limb, options.live);
      if (std::memcmp(&got, &expected, sizeof(uint4)) != 0) {
        throw std::runtime_error("GPU DPF reconstruction failed");
      }
    }
  }
}

Timings benchmark_dense(const Options& options, const std::vector<int>& targets,
                        const uint4* table, int multiprocessors) {
  double client_ms = 0;
  DenseSelectorPair selectors =
      make_dense_selectors(options, targets, &client_ms);
  const std::vector<uint32_t>& selector0 = selectors.server0;
  const std::vector<uint32_t>& selector1 = selectors.server1;
  const size_t selector_bytes = selector0.size() * sizeof(uint32_t);
  const int blocks_per_query = std::max(1, multiprocessors * 4);

  const auto context_started = std::chrono::steady_clock::now();
  uint32_t* device_selector = nullptr;
  uint4* partial = nullptr;
  uint4* output = nullptr;
  DEFRA_CUDA(cudaMalloc(&device_selector, selector_bytes));
  DEFRA_CUDA(cudaMalloc(&partial, static_cast<size_t>(options.batch) *
                                      blocks_per_query * DEFRA_LIMBS * sizeof(uint4)));
  DEFRA_CUDA(cudaMalloc(&output, static_cast<size_t>(options.batch) *
                                     DEFRA_LIMBS * sizeof(uint4)));
  DEFRA_CUDA(cudaDeviceSynchronize());
  const double context_ms = milliseconds(
      std::chrono::steady_clock::now() - context_started);

  auto launch = [&]() {
    dense_partial_kernel<<<options.batch * blocks_per_query, kThreads>>>(
        table, device_selector, partial, options.entries, blocks_per_query,
        options.batch);
    dense_finish_kernel<<<options.batch, kThreads>>>(
        partial, output, blocks_per_query, options.batch);
  };

  cudaEvent_t start;
  cudaEvent_t stop;
  DEFRA_CUDA(cudaEventCreate(&start));
  DEFRA_CUDA(cudaEventCreate(&stop));

  std::vector<uint4> host0(static_cast<size_t>(options.batch) * DEFRA_LIMBS);
  std::vector<uint4> host1(host0.size());
  auto first_server = [&](const std::vector<uint32_t>& selector,
                          std::vector<uint4>& host,
                          double* h2d_ms, double* server_ms,
                          double* d2h_ms) {
    auto started = std::chrono::steady_clock::now();
    DEFRA_CUDA(cudaMemcpy(device_selector, selector.data(), selector_bytes,
                          cudaMemcpyHostToDevice));
    *h2d_ms = milliseconds(std::chrono::steady_clock::now() - started);
    started = std::chrono::steady_clock::now();
    launch();
    DEFRA_CUDA(cudaDeviceSynchronize());
    *server_ms = milliseconds(std::chrono::steady_clock::now() - started);
    started = std::chrono::steady_clock::now();
    DEFRA_CUDA(cudaMemcpy(host.data(), output, host.size() * sizeof(uint4),
                          cudaMemcpyDeviceToHost));
    *d2h_ms = milliseconds(std::chrono::steady_clock::now() - started);
  };
  double first_h2d0_ms = 0.0;
  double first_h2d1_ms = 0.0;
  double first_server0_ms = 0.0;
  double first_server1_ms = 0.0;
  double first_d2h0_ms = 0.0;
  double first_d2h1_ms = 0.0;
  first_server(selector0, host0, &first_h2d0_ms, &first_server0_ms,
               &first_d2h0_ms);
  first_server(selector1, host1, &first_h2d1_ms, &first_server1_ms,
               &first_d2h1_ms);
  verify_dense(options, targets, host0, host1);

  DEFRA_CUDA(cudaEventRecord(start));
  launch();
  DEFRA_CUDA(cudaEventRecord(stop));
  DEFRA_CUDA(cudaEventSynchronize(stop));
  const int repetitions = calibrated_repetitions(
      elapsed_event(start, stop), options.min_sample_ms);

  std::vector<double> h2d;
  std::vector<double> server0_ms;
  std::vector<double> server1_ms;
  std::vector<double> d2h;
  PowerSampler power;
  power.start();
  for (int sample = 0; sample < options.samples; ++sample) {
    auto run_server = [&](const std::vector<uint32_t>& selector,
                          std::vector<uint4>& host,
                          std::vector<double>& server_samples) {
      DEFRA_CUDA(cudaEventRecord(start));
      for (int repetition = 0; repetition < repetitions; ++repetition) {
        DEFRA_CUDA(cudaMemcpyAsync(device_selector, selector.data(), selector_bytes,
                                   cudaMemcpyHostToDevice));
      }
      DEFRA_CUDA(cudaEventRecord(stop));
      DEFRA_CUDA(cudaEventSynchronize(stop));
      h2d.push_back(elapsed_event(start, stop) / repetitions);

      DEFRA_CUDA(cudaEventRecord(start));
      for (int repetition = 0; repetition < repetitions; ++repetition) {
        launch();
      }
      DEFRA_CUDA(cudaEventRecord(stop));
      DEFRA_CUDA(cudaEventSynchronize(stop));
      server_samples.push_back(elapsed_event(start, stop) / repetitions);

      DEFRA_CUDA(cudaEventRecord(start));
      DEFRA_CUDA(cudaMemcpyAsync(host.data(), output,
                                 host.size() * sizeof(uint4),
                                 cudaMemcpyDeviceToHost));
      DEFRA_CUDA(cudaEventRecord(stop));
      DEFRA_CUDA(cudaEventSynchronize(stop));
      d2h.push_back(elapsed_event(start, stop));
    };
    run_server(selector0, host0, server0_ms);
    run_server(selector1, host1, server1_ms);
    verify_dense(options, targets, host0, host1);
  }
  power.stop();

  Timings result;
  result.client_ms = client_ms;
  result.first_online_measured = true;
  result.server_context_ms = context_ms;
  result.first_h2d_aggregate_ms = first_h2d0_ms + first_h2d1_ms;
  result.first_server0_ms = first_server0_ms;
  result.first_server1_ms = first_server1_ms;
  result.first_aggregate_server_ms = first_server0_ms + first_server1_ms;
  result.first_parallel_server_ms =
      std::max(first_server0_ms, first_server1_ms);
  result.first_d2h_aggregate_ms = first_d2h0_ms + first_d2h1_ms;
  result.h2d_p50_ms = median(h2d) * 2.0;
  result.server0_p50_ms = median(server0_ms);
  result.server1_p50_ms = median(server1_ms);
  result.aggregate_p50_ms = result.server0_p50_ms + result.server1_p50_ms;
  result.wall_p50_ms = std::max(result.server0_p50_ms, result.server1_p50_ms);
  result.d2h_p50_ms = median(d2h) * 2.0;
  result.repetitions = repetitions;
  power.apply(&result, static_cast<size_t>(options.samples) * repetitions *
                           options.batch);
  cudaEventDestroy(start);
  cudaEventDestroy(stop);
  cudaFree(output);
  cudaFree(partial);
  cudaFree(device_selector);
  return result;
}

Timings benchmark_packed_presence(const Options& options,
                                  const std::vector<int>& targets,
                                  int multiprocessors) {
  if (!options.live) {
    throw std::runtime_error("packed-presence control is live-only");
  }
  double client_ms = 0;
  DenseSelectorPair selectors =
      make_dense_selectors(options, targets, &client_ms);
  const std::vector<uint32_t>& selector0 = selectors.server0;
  const std::vector<uint32_t>& selector1 = selectors.server1;
  const size_t selector_bytes = selector0.size() * sizeof(uint32_t);
  const int words = options.entries / 32;
  const int blocks_per_query = std::max(
      1, std::min(multiprocessors * 2,
                  (words + kThreads - 1) / kThreads));

  std::vector<uint32_t> host_presence(words, 0);
  const int depth = log2_entries(options.entries);
  for (int logical = 0; logical < options.entries; ++logical) {
    if (logical % 7 != 0) {
      continue;
    }
    const uint32_t physical =
        reverse_bits(static_cast<uint32_t>(logical)) >> (32 - depth);
    host_presence[physical / 32] |= 1u << (physical & 31);
  }

  uint32_t* presence = nullptr;
  uint32_t* device_selector = nullptr;
  uint32_t* partial = nullptr;
  uint32_t* output = nullptr;
  DEFRA_CUDA(cudaMalloc(&presence, host_presence.size() * sizeof(uint32_t)));
  DEFRA_CUDA(cudaMalloc(&device_selector, selector_bytes));
  DEFRA_CUDA(cudaMalloc(&partial, static_cast<size_t>(options.batch) *
                                      blocks_per_query * sizeof(uint32_t)));
  DEFRA_CUDA(cudaMalloc(&output, static_cast<size_t>(options.batch) *
                                     sizeof(uint32_t)));
  DEFRA_CUDA(cudaMemcpy(presence, host_presence.data(),
                        host_presence.size() * sizeof(uint32_t),
                        cudaMemcpyHostToDevice));
  auto launch = [&]() {
    presence_partial_kernel<<<options.batch * blocks_per_query, kThreads>>>(
        presence, device_selector, partial, words, blocks_per_query,
        options.batch);
    presence_finish_kernel<<<options.batch, kThreads>>>(
        partial, output, blocks_per_query, options.batch);
  };

  DEFRA_CUDA(cudaMemcpy(device_selector, selector0.data(), selector_bytes,
                        cudaMemcpyHostToDevice));
  launch();
  DEFRA_CUDA(cudaDeviceSynchronize());
  cudaEvent_t start;
  cudaEvent_t stop;
  DEFRA_CUDA(cudaEventCreate(&start));
  DEFRA_CUDA(cudaEventCreate(&stop));
  DEFRA_CUDA(cudaEventRecord(start));
  launch();
  DEFRA_CUDA(cudaEventRecord(stop));
  DEFRA_CUDA(cudaEventSynchronize(stop));
  const int repetitions = calibrated_repetitions(
      elapsed_event(start, stop), options.min_sample_ms);

  std::vector<double> h2d;
  std::vector<double> server0_ms;
  std::vector<double> server1_ms;
  std::vector<double> d2h;
  std::vector<uint32_t> host0(options.batch);
  std::vector<uint32_t> host1(options.batch);
  PowerSampler power;
  power.start();
  for (int sample = 0; sample < options.samples; ++sample) {
    auto run_server = [&](const std::vector<uint32_t>& selector,
                          std::vector<uint32_t>& host,
                          std::vector<double>& server_samples) {
      DEFRA_CUDA(cudaEventRecord(start));
      for (int repetition = 0; repetition < repetitions; ++repetition) {
        DEFRA_CUDA(cudaMemcpyAsync(device_selector, selector.data(),
                                   selector_bytes, cudaMemcpyHostToDevice));
      }
      DEFRA_CUDA(cudaEventRecord(stop));
      DEFRA_CUDA(cudaEventSynchronize(stop));
      h2d.push_back(elapsed_event(start, stop) / repetitions);

      DEFRA_CUDA(cudaEventRecord(start));
      for (int repetition = 0; repetition < repetitions; ++repetition) {
        launch();
      }
      DEFRA_CUDA(cudaEventRecord(stop));
      DEFRA_CUDA(cudaEventSynchronize(stop));
      server_samples.push_back(elapsed_event(start, stop) / repetitions);

      DEFRA_CUDA(cudaEventRecord(start));
      DEFRA_CUDA(cudaMemcpyAsync(host.data(), output,
                                 host.size() * sizeof(uint32_t),
                                 cudaMemcpyDeviceToHost));
      DEFRA_CUDA(cudaEventRecord(stop));
      DEFRA_CUDA(cudaEventSynchronize(stop));
      d2h.push_back(elapsed_event(start, stop));
    };
    run_server(selector0, host0, server0_ms);
    run_server(selector1, host1, server1_ms);
    for (int query = 0; query < options.batch; ++query) {
      const uint32_t got = (host0[query] ^ host1[query]) & 1u;
      const uint32_t expected = targets[query] % 7 == 0 ? 1u : 0u;
      if (got != expected) {
        throw std::runtime_error("packed-presence reconstruction failed");
      }
    }
  }
  power.stop();

  Timings result;
  result.client_ms = client_ms;
  result.h2d_p50_ms = median(h2d) * 2.0;
  result.server0_p50_ms = median(server0_ms);
  result.server1_p50_ms = median(server1_ms);
  result.aggregate_p50_ms = result.server0_p50_ms + result.server1_p50_ms;
  result.wall_p50_ms = std::max(result.server0_p50_ms, result.server1_p50_ms);
  result.d2h_p50_ms = median(d2h) * 2.0;
  result.repetitions = repetitions;
  power.apply(&result, static_cast<size_t>(options.samples) * repetitions *
                           options.batch);
  cudaEventDestroy(start);
  cudaEventDestroy(stop);
  cudaFree(output);
  cudaFree(partial);
  cudaFree(device_selector);
  cudaFree(presence);
  return result;
}

Timings benchmark_dpf(const Options& options, const std::vector<int>& targets,
                      const uint4* table) {
  double client_ms = 0;
  const DpfKeyPair keys = make_dpf_keys(options, targets, &client_ms);
  const std::vector<SeedsCodewordsFlatGPU>& keys0 = keys.server0;
  const std::vector<SeedsCodewordsFlatGPU>& keys1 = keys.server1;
  const size_t key_bytes = keys0.size() * sizeof(SeedsCodewordsFlatGPU);

  const auto context_started = std::chrono::steady_clock::now();
  SeedsCodewordsFlatGPU* device_keys = nullptr;
  uint4* output = nullptr;
  DEFRA_CUDA(cudaMalloc(&device_keys, key_bytes));
  DEFRA_CUDA(cudaMalloc(&output, static_cast<size_t>(options.batch) *
                                     DEFRA_LIMBS * sizeof(uint4)));
  dpf_hybrid_initialize(options.batch, options.entries);
  DEFRA_CUDA(cudaDeviceSynchronize());
  const double context_ms = milliseconds(
      std::chrono::steady_clock::now() - context_started);
  auto launch = [&]() {
    dpf_hybrid<CHACHA20>(device_keys, output,
                         const_cast<uint4*>(table), options.batch,
                         options.entries, nullptr);
  };

  cudaEvent_t start;
  cudaEvent_t stop;
  DEFRA_CUDA(cudaEventCreate(&start));
  DEFRA_CUDA(cudaEventCreate(&stop));

  std::vector<uint4> host0(static_cast<size_t>(options.batch) * DEFRA_LIMBS);
  std::vector<uint4> host1(host0.size());
  auto first_server = [&](const std::vector<SeedsCodewordsFlatGPU>& keys,
                          std::vector<uint4>& host,
                          double* h2d_ms, double* server_ms,
                          double* d2h_ms) {
    auto started = std::chrono::steady_clock::now();
    DEFRA_CUDA(cudaMemcpy(device_keys, keys.data(), key_bytes,
                          cudaMemcpyHostToDevice));
    *h2d_ms = milliseconds(std::chrono::steady_clock::now() - started);
    started = std::chrono::steady_clock::now();
    launch();
    DEFRA_CUDA(cudaDeviceSynchronize());
    *server_ms = milliseconds(std::chrono::steady_clock::now() - started);
    started = std::chrono::steady_clock::now();
    DEFRA_CUDA(cudaMemcpy(host.data(), output, host.size() * sizeof(uint4),
                          cudaMemcpyDeviceToHost));
    *d2h_ms = milliseconds(std::chrono::steady_clock::now() - started);
  };
  double first_h2d0_ms = 0.0;
  double first_h2d1_ms = 0.0;
  double first_server0_ms = 0.0;
  double first_server1_ms = 0.0;
  double first_d2h0_ms = 0.0;
  double first_d2h1_ms = 0.0;
  first_server(keys0, host0, &first_h2d0_ms, &first_server0_ms,
               &first_d2h0_ms);
  first_server(keys1, host1, &first_h2d1_ms, &first_server1_ms,
               &first_d2h1_ms);
  verify_dpf(options, targets, host0, host1);

  DEFRA_CUDA(cudaEventRecord(start));
  launch();
  DEFRA_CUDA(cudaEventRecord(stop));
  DEFRA_CUDA(cudaEventSynchronize(stop));
  const int repetitions = calibrated_repetitions(
      elapsed_event(start, stop), options.min_sample_ms);

  std::vector<double> h2d;
  std::vector<double> server0_ms;
  std::vector<double> server1_ms;
  std::vector<double> d2h;
  PowerSampler power;
  power.start();
  for (int sample = 0; sample < options.samples; ++sample) {
    auto run_server = [&](const std::vector<SeedsCodewordsFlatGPU>& keys,
                          std::vector<uint4>& host,
                          std::vector<double>& server_samples) {
      DEFRA_CUDA(cudaEventRecord(start));
      for (int repetition = 0; repetition < repetitions; ++repetition) {
        DEFRA_CUDA(cudaMemcpyAsync(device_keys, keys.data(), key_bytes,
                                   cudaMemcpyHostToDevice));
      }
      DEFRA_CUDA(cudaEventRecord(stop));
      DEFRA_CUDA(cudaEventSynchronize(stop));
      h2d.push_back(elapsed_event(start, stop) / repetitions);

      DEFRA_CUDA(cudaEventRecord(start));
      for (int repetition = 0; repetition < repetitions; ++repetition) {
        launch();
      }
      DEFRA_CUDA(cudaEventRecord(stop));
      DEFRA_CUDA(cudaEventSynchronize(stop));
      server_samples.push_back(elapsed_event(start, stop) / repetitions);

      DEFRA_CUDA(cudaEventRecord(start));
      DEFRA_CUDA(cudaMemcpyAsync(host.data(), output,
                                 host.size() * sizeof(uint4),
                                 cudaMemcpyDeviceToHost));
      DEFRA_CUDA(cudaEventRecord(stop));
      DEFRA_CUDA(cudaEventSynchronize(stop));
      d2h.push_back(elapsed_event(start, stop));
    };
    run_server(keys0, host0, server0_ms);
    run_server(keys1, host1, server1_ms);
    verify_dpf(options, targets, host0, host1);
  }
  power.stop();

  Timings result;
  result.client_ms = client_ms;
  result.first_online_measured = true;
  result.server_context_ms = context_ms;
  result.first_h2d_aggregate_ms = first_h2d0_ms + first_h2d1_ms;
  result.first_server0_ms = first_server0_ms;
  result.first_server1_ms = first_server1_ms;
  result.first_aggregate_server_ms = first_server0_ms + first_server1_ms;
  result.first_parallel_server_ms =
      std::max(first_server0_ms, first_server1_ms);
  result.first_d2h_aggregate_ms = first_d2h0_ms + first_d2h1_ms;
  result.h2d_p50_ms = median(h2d) * 2.0;
  result.server0_p50_ms = median(server0_ms);
  result.server1_p50_ms = median(server1_ms);
  result.aggregate_p50_ms = result.server0_p50_ms + result.server1_p50_ms;
  result.wall_p50_ms = std::max(result.server0_p50_ms, result.server1_p50_ms);
  result.d2h_p50_ms = median(d2h) * 2.0;
  result.repetitions = repetitions;
  power.apply(&result, static_cast<size_t>(options.samples) * repetitions *
                           options.batch);
  cudaEventDestroy(start);
  cudaEventDestroy(stop);
  dpf_hybrid_deinitialize();
  cudaFree(output);
  cudaFree(device_keys);
  return result;
}

VisibleTimings benchmark_visible100(const Options& options,
                                    const std::vector<int>& targets) {
  if (!options.live) {
    throw std::runtime_error("visible-100 control is live-only");
  }
  const auto registration_started = std::chrono::steady_clock::now();
  std::vector<uint32_t> candidates(
      static_cast<size_t>(options.batch) * kVisibleCandidates);
  for (int query = 0; query < options.batch; ++query) {
    for (int candidate = 0; candidate < kVisibleCandidates; ++candidate) {
      const uint32_t offset = static_cast<uint32_t>(candidate + 1) * 613u;
      candidates[static_cast<size_t>(query) * kVisibleCandidates + candidate] =
          (static_cast<uint32_t>(targets[query]) + offset) &
          static_cast<uint32_t>(options.entries - 1);
    }
    candidates[static_cast<size_t>(query) * kVisibleCandidates +
               kVisibleTargetSlot] = static_cast<uint32_t>(targets[query]);
  }
  const double registration_ms = milliseconds(
      std::chrono::steady_clock::now() - registration_started);

  std::vector<uint4> table(options.entries);
  for (int ordinal = 0; ordinal < options.entries; ++ordinal) {
    table[ordinal] = record_limb(static_cast<uint32_t>(ordinal), 0, true);
  }
  std::vector<uint4> response(candidates.size());
  auto server_once = [&]() {
    for (size_t index = 0; index < candidates.size(); ++index) {
      response[index] = table[candidates[index]];
    }
  };
  server_once();
  for (int query = 0; query < options.batch; ++query) {
    const uint4 got = response[static_cast<size_t>(query) * kVisibleCandidates +
                               kVisibleTargetSlot];
    const uint4 expected = record_limb(targets[query], 0, true);
    if (std::memcmp(&got, &expected, sizeof(uint4)) != 0) {
      throw std::runtime_error("visible-100 lookup failed");
    }
  }

  const auto calibration_started = std::chrono::steady_clock::now();
  server_once();
  const double first_ms = milliseconds(
      std::chrono::steady_clock::now() - calibration_started);
  const int repetitions = calibrated_repetitions(first_ms,
                                                  options.min_sample_ms);
  std::vector<double> server_ms;
  std::vector<double> client_ms;
  for (int sample = 0; sample < options.samples; ++sample) {
    auto started = std::chrono::steady_clock::now();
    for (int repetition = 0; repetition < repetitions; ++repetition) {
      server_once();
      const uint4 observed = response[static_cast<size_t>(repetition) %
                                      response.size()];
      visible_sink ^= static_cast<uint64_t>(observed.x) << 32 | observed.y;
    }
    server_ms.push_back(milliseconds(std::chrono::steady_clock::now() - started) /
                        repetitions);

    started = std::chrono::steady_clock::now();
    for (int repetition = 0; repetition < repetitions; ++repetition) {
      for (int query = 0; query < options.batch; ++query) {
        const uint4 selected =
            response[static_cast<size_t>(query) * kVisibleCandidates +
                     kVisibleTargetSlot];
        visible_sink ^= static_cast<uint64_t>(selected.x) << 32 | selected.y;
      }
    }
    client_ms.push_back(milliseconds(std::chrono::steady_clock::now() - started) /
                        repetitions);
  }
  return VisibleTimings{registration_ms, median(server_ms), median(client_ms),
                        repetitions};
}

void print_timing(const char* name, const Timings& timing, size_t upload_bytes,
                  size_t response_bytes, int batch) {
  std::printf(
      "\"%s\":{\"client_batch_ms\":%.6f,\"server_context_ms\":%.6f,"
      "\"first_online\":{\"measured\":%s,\"gpu_h2d_aggregate_ms\":%.6f,"
      "\"server0_ms\":%.6f,\"server1_ms\":%.6f,"
      "\"aggregate_server_ms\":%.6f,\"parallel_server_ms\":%.6f,"
      "\"gpu_d2h_aggregate_ms\":%.6f},\"gpu_h2d_p50_ms\":%.6f,"
      "\"server0_p50_ms\":%.6f,\"server1_p50_ms\":%.6f,"
      "\"aggregate_server_p50_ms\":%.6f,\"parallel_wall_p50_ms\":%.6f,"
      "\"aggregate_server_ms_per_query\":%.6f,"
      "\"parallel_queries_per_second\":%.3f,\"gpu_d2h_p50_ms\":%.6f,"
      "\"mean_gpu_power_watts\":%.3f,\"peak_gpu_power_watts\":%.3f,"
      "\"approximate_gpu_joules_per_query\":%.6f,\"power_samples\":%zu,"
      "\"aggregate_upload_bytes\":%zu,\"aggregate_response_bytes\":%zu,"
      "\"calibrated_repetitions\":%d}",
      name, timing.client_ms, timing.server_context_ms,
      timing.first_online_measured ? "true" : "false",
      timing.first_h2d_aggregate_ms, timing.first_server0_ms,
      timing.first_server1_ms, timing.first_aggregate_server_ms,
      timing.first_parallel_server_ms, timing.first_d2h_aggregate_ms,
      timing.h2d_p50_ms, timing.server0_p50_ms,
      timing.server1_p50_ms, timing.aggregate_p50_ms, timing.wall_p50_ms,
      timing.aggregate_p50_ms / batch,
      batch * 1000.0 / timing.wall_p50_ms, timing.d2h_p50_ms,
      timing.mean_gpu_power_watts, timing.peak_gpu_power_watts,
      timing.approximate_gpu_joules_per_query, timing.power_samples,
      upload_bytes, response_bytes, timing.repetitions);
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const Options options = parse_options(argc, argv);
    int device = 0;
    cudaDeviceProp properties{};
    DEFRA_CUDA(cudaGetDevice(&device));
    DEFRA_CUDA(cudaGetDeviceProperties(&properties, device));
    const size_t table_bytes = static_cast<size_t>(options.entries) *
                               DEFRA_LIMBS * sizeof(uint4);
    const auto table_started = std::chrono::steady_clock::now();
    uint4* table = nullptr;
    DEFRA_CUDA(cudaMalloc(&table, table_bytes));
    const int depth = log2_entries(options.entries);
    initialize_table_kernel<<<(options.entries + 255) / 256, 256>>>(
        table, options.entries, depth, options.live);
    DEFRA_CUDA(cudaDeviceSynchronize());
    const double table_materialize_ms = milliseconds(
        std::chrono::steady_clock::now() - table_started);

    const std::vector<int> targets = target_ordinals(options);
    Timings dense;
    Timings dpf;
    if (options.dpf_first) {
      dpf = benchmark_dpf(options, targets, table);
      dense = benchmark_dense(options, targets, table,
                              properties.multiProcessorCount);
    } else {
      dense = benchmark_dense(options, targets, table,
                              properties.multiProcessorCount);
      dpf = benchmark_dpf(options, targets, table);
    }
    const Timings packed_presence = options.live
        ? benchmark_packed_presence(options, targets,
                                    properties.multiProcessorCount)
        : Timings{};
    const VisibleTimings visible =
        options.live ? benchmark_visible100(options, targets) : VisibleTimings{};
    const int useful_bytes =
        options.live ? kUsefulLiveBytes : kUsefulSnapshotBytes;
    const size_t dense_upload =
        static_cast<size_t>(options.batch) * options.entries / 8 * 2;
    const size_t dpf_upload = static_cast<size_t>(options.batch) *
                              sizeof(SeedsCodewordsFlatGPU) * 2;
    const size_t response =
        static_cast<size_t>(options.batch) * useful_bytes * 2;

    std::printf("{");
    std::printf(
        "\"schema\":\"defradb-gpu-pir-comparison-v3\","
        "\"upstream_commit\":\"%s\",\"gpu\":\"%s\","
        "\"compute_capability\":\"%d.%d\",\"mode\":\"%s\","
        "\"entries\":%d,\"batch\":%d,\"samples\":%d,"
        "\"protocol_order\":\"%s\","
        "\"useful_row_bytes\":%d,\"physical_row_bytes\":%zu,"
        "\"table_bytes_per_replica\":%zu,"
        "\"gpu_table_materialize_ms_per_replica\":%.6f,"
        "\"packed_presence_table_bytes_per_replica\":%zu,"
        "\"dpf_key_bytes_per_server\":%zu,",
        kUpstreamCommit, properties.name, properties.major, properties.minor,
        options.live ? "live_epoch_histogram" : "snapshot", options.entries,
        options.batch, options.samples,
        options.dpf_first ? "dpf-first" : "dense-first", useful_bytes,
        DEFRA_LIMBS * sizeof(uint4), table_bytes, table_materialize_ms,
        options.live ? static_cast<size_t>(options.entries) / 8 : 0,
        sizeof(SeedsCodewordsFlatGPU));
    print_timing("dense_xor", dense, dense_upload, response, options.batch);
    std::printf(",");
    print_timing("gpu_dpf", dpf, dpf_upload, response, options.batch);
    if (options.live) {
      std::printf(",");
      const size_t packed_response =
          static_cast<size_t>(options.batch) * 2;
      print_timing("packed_presence_dense", packed_presence, dense_upload,
                   packed_response, options.batch);
      const size_t visible_registration = static_cast<size_t>(options.batch) *
                                          kVisibleCandidates * sizeof(uint32_t);
      const size_t visible_response = static_cast<size_t>(options.batch) *
                                      kVisibleCandidates * useful_bytes;
      std::printf(
          ",\"visible_100\":{\"registration_client_batch_ms\":%.6f,"
          "\"server_p50_ms\":%.6f,\"server_ms_per_query\":%.6f,"
          "\"queries_per_second\":%.3f,\"client_filter_p50_ms\":%.6f,"
          "\"registration_upload_bytes\":%zu,"
          "\"response_bytes_per_epoch\":%zu,"
          "\"calibrated_repetitions\":%d}",
          visible.registration_client_ms, visible.server_p50_ms,
          visible.server_p50_ms / options.batch,
          options.batch * 1000.0 / visible.server_p50_ms,
          visible.client_filter_p50_ms, visible_registration,
          visible_response, visible.repetitions);
    }
    std::printf(
        ",\"correctness\":{\"dense\":true,\"dpf\":true,"
        "\"packed_presence_dense\":%s,"
        "\"visible_100\":%s},"
        "\"scope\":\"first_online is the first H2D/answer/D2H after table and protocol-context construction; warm server kernels follow calibration; strict replicas execute sequentially on one GPU, while visible_100 uses one host-CPU server; aggregate is the strict replica sum and parallel wall is their max; HTTP, network, queueing, persistence, and keyword-to-ordinal lookup excluded\"}\n",
        options.live ? "true" : "null",
        options.live ? "true" : "null");
    cudaFree(table);
    return 0;
  } catch (const std::exception& error) {
    std::fprintf(stderr, "gpu-pir benchmark failed: %s\n", error.what());
    return 1;
  }
}
