// Fresh complete requests. The included artifact's replay benchmark is not run.
#include <sys/random.h>
#include <sys/resource.h>
#include <time.h>
#include <cerrno>
#include <fstream>
#include <stdexcept>
static void secure_fill(void* data, size_t size) {
  auto* p = static_cast<unsigned char*>(data);
  while (size) { const auto n = getrandom(p, size, 0); if(n<0 && errno==EINTR)continue;
    if(n<=0)throw std::runtime_error("getrandom failed"); p+=n;size-=n; }
}
#define main replay_benchmark_main
#include "benchmark.cu"
#undef main

static double process_cpu_ms() { timespec t{}; if(clock_gettime(CLOCK_PROCESS_CPUTIME_ID,&t))throw std::runtime_error("CPU clock");return t.tv_sec*1000.+t.tv_nsec/1e6; }
template<class F> static double gpu_event(F operation) {
  cudaEvent_t a,b;DEFRA_CUDA(cudaEventCreate(&a));DEFRA_CUDA(cudaEventCreate(&b));
  DEFRA_CUDA(cudaEventRecord(a));operation();DEFRA_CUDA(cudaEventRecord(b));DEFRA_CUDA(cudaEventSynchronize(b));
  const auto ms=elapsed_event(a,b);DEFRA_CUDA(cudaEventDestroy(a));DEFRA_CUDA(cudaEventDestroy(b));return ms;
}
__global__ void gather_rows(const uint4* table,const int* indices,uint4* out,int count,int rows) {
  int i=blockIdx.x*blockDim.x+threadIdx.x;
  if(i<count*DEFRA_LIMBS){int physical=__brev(static_cast<unsigned>(indices[i/DEFRA_LIMBS]))>>(32-__ffs(rows)+1);
    // initialize_table_kernel uses limb-major storage.
    out[i]=table[static_cast<size_t>(i%DEFRA_LIMBS)*rows+physical];}
}

int main(int argc,char** argv) {
  try {
    if(argc!=6)throw std::runtime_error("usage: binary dense|dpf|public|decoy rows batch queries output.json");
    const std::string mode=argv[1];Options o;o.entries=std::stoi(argv[2]);o.batch=std::stoi(argv[3]);o.samples=std::stoi(argv[4]);
    if(o.entries<256 || (o.entries&(o.entries-1)) || o.batch<1 || o.batch>512 || o.samples<1 || o.samples>10000)throw std::runtime_error("invalid dimensions");
    if(mode!="dense" && mode!="dpf" && mode!="public" && mode!="decoy")throw std::runtime_error("unknown candidate");
    bool visible=mode=="public"||mode=="decoy";int roles=visible?1:2,slots=mode=="decoy"?100:1;
    const size_t table_bytes=static_cast<size_t>(o.entries)*DEFRA_LIMBS*sizeof(uint4);
    double cpu=process_cpu_ms(),server_cpu=0,client_cpu=0,setup_gpu=0;
    cudaDeviceProp properties{};DEFRA_CUDA(cudaGetDeviceProperties(&properties,0));
    size_t free_mem,total_mem;DEFRA_CUDA(cudaMemGetInfo(&free_mem,&total_mem));
    if(table_bytes*roles+static_cast<size_t>(o.batch)*o.entries*32>free_mem*3/4)throw std::runtime_error("GPU resident preflight exceeds 75% available VRAM");
    std::vector<uint4*> tables(roles,nullptr);
    for(auto& table:tables){DEFRA_CUDA(cudaMalloc(&table,table_bytes));setup_gpu+=gpu_event([&]{initialize_table_kernel<<<(o.entries+255)/256,256>>>(table,o.entries,log2_entries(o.entries),false);});}
    int blocks=std::max(1,properties.multiProcessorCount*4);
    size_t key_bytes=mode=="dpf"?o.batch*sizeof(SeedsCodewordsFlatGPU):visible?o.batch*slots*sizeof(int):static_cast<size_t>(o.batch)*o.entries/8;
    void* keys=nullptr;uint4 *out=nullptr,*partial=nullptr;
    DEFRA_CUDA(cudaMalloc(&keys,key_bytes));DEFRA_CUDA(cudaMalloc(&out,static_cast<size_t>(o.batch)*slots*DEFRA_LIMBS*sizeof(uint4)));
    if(mode=="dense")DEFRA_CUDA(cudaMalloc(&partial,static_cast<size_t>(o.batch)*blocks*DEFRA_LIMBS*sizeof(uint4)));
    if(mode=="dpf")dpf_hybrid_initialize(o.batch,o.entries);
    DEFRA_CUDA(cudaDeviceSynchronize());server_cpu+=process_cpu_ms()-cpu;
    double total_gpu=setup_gpu;size_t upload=0,download=0;
    std::ofstream result(argv[5]);result<<"{\"schema\":\"pir-gpu-complete-work-v1\",\"samples\":[";
    for(int q=0;q<o.samples;q++) {
      auto wall=std::chrono::steady_clock::now();cpu=process_cpu_ms();
      std::vector<int> targets(o.batch);for(int b=0;b<o.batch;b++)targets[b]=(q*31337+b*7919)%o.entries;
      DenseSelectorPair dense;DpfKeyPair dpf;std::vector<int> candidates(o.batch*slots),target_slots(o.batch);
      double unused;
      if(mode=="dpf")dpf=make_dpf_keys(o,targets,&unused); // adapter patches every upstream secret random draw to getrandom
      else if(mode=="dense") {
        dense.server0.resize(key_bytes/4);secure_fill(dense.server0.data(),key_bytes);dense.server1=dense.server0;
        for(int b=0;b<o.batch;b++){auto physical=reverse_bits(targets[b])>>(32-log2_entries(o.entries));dense.server1[static_cast<size_t>(b)*o.entries/32+physical/32]^=1u<<(physical%32);}
      } else {
        secure_fill(candidates.data(),candidates.size()*sizeof(int));
        for(auto& i:candidates)i=static_cast<unsigned>(i)%o.entries;
        for(int b=0;b<o.batch;b++){unsigned r;secure_fill(&r,sizeof(r));target_slots[b]=r%slots;candidates[b*slots+target_slots[b]]=targets[b];}
      }
      double query_client=process_cpu_ms()-cpu;std::vector<uint4> replies[2];double active[2]={0,0},h2d=0,d2h=0,query_server=0;
      for(int role=0;role<roles;role++) {
        cpu=process_cpu_ms();replies[role].resize(static_cast<size_t>(o.batch)*slots*DEFRA_LIMBS);
        const void* host=mode=="dpf"?static_cast<void*>((role?dpf.server1:dpf.server0).data()):visible?static_cast<void*>(candidates.data()):static_cast<void*>((role?dense.server1:dense.server0).data());
        h2d+=gpu_event([&]{DEFRA_CUDA(cudaMemcpy(keys,host,key_bytes,cudaMemcpyHostToDevice));});
        active[role]=gpu_event([&]{
          if(mode=="dpf")dpf_hybrid<CHACHA20>(static_cast<SeedsCodewordsFlatGPU*>(keys),out,tables[role],o.batch,o.entries,nullptr);
          else if(visible)gather_rows<<<(o.batch*slots*DEFRA_LIMBS+127)/128,128>>>(tables[role],static_cast<int*>(keys),out,o.batch*slots,o.entries);
          else {dense_partial_kernel<<<o.batch*blocks,kThreads>>>(tables[role],static_cast<uint32_t*>(keys),partial,o.entries,blocks,o.batch);dense_finish_kernel<<<o.batch,kThreads>>>(partial,out,blocks,o.batch);}
          DEFRA_CUDA(cudaGetLastError());
        });
        d2h+=gpu_event([&]{DEFRA_CUDA(cudaMemcpy(replies[role].data(),out,replies[role].size()*sizeof(uint4),cudaMemcpyDeviceToHost));});
        query_server+=process_cpu_ms()-cpu;upload+=key_bytes;download+=replies[role].size()*sizeof(uint4);
      }
      cpu=process_cpu_ms();
      if(mode=="dpf")verify_dpf(o,targets,replies[0],replies[1]);
      else if(!visible)verify_dense(o,targets,replies[0],replies[1]);
      else for(int b=0;b<o.batch;b++)for(int limb=0;limb<DEFRA_LIMBS;limb++){
        uint4 expected=record_limb(targets[b],limb,false),answer=replies[0][(b*slots+target_slots[b])*DEFRA_LIMBS+limb];
        if(std::memcmp(&expected,&answer,sizeof(expected)))throw std::runtime_error("visible reconstruction");}
      query_client+=process_cpu_ms()-cpu;client_cpu+=query_client;server_cpu+=query_server;total_gpu+=active[0]+active[1]+h2d+d2h;
      if(q)result<<",";
      result<<"{\"logical_queries\":"<<o.batch<<",\"server_cpu_ms\":"<<query_server<<",\"client_cpu_ms\":"<<query_client<<",\"gpu_role_ms\":["<<active[0]<<","<<active[1]<<"],\"aggregate_gpu_compute_ms\":"<<active[0]+active[1]<<",\"h2d_ms\":"<<h2d<<",\"d2h_ms\":"<<d2h<<",\"verified_latency_ms\":"<<milliseconds(std::chrono::steady_clock::now()-wall)<<"}";
    }
    cpu=process_cpu_ms();if(mode=="dpf")dpf_hybrid_deinitialize();cudaFree(keys);cudaFree(out);cudaFree(partial);for(auto table:tables)cudaFree(table);DEFRA_CUDA(cudaDeviceSynchronize());server_cpu+=process_cpu_ms()-cpu;
    rusage usage{};getrusage(RUSAGE_SELF,&usage);size_t count=static_cast<size_t>(o.samples)*o.batch;
    result<<"],\"completed_logical_queries\":"<<count<<",\"server_cpu_ms\":"<<server_cpu<<",\"server_cpu_ms_per_query\":"<<server_cpu/count<<",\"client_cpu_ms\":"<<client_cpu<<",\"gpu_active_ms\":"<<total_gpu<<",\"setup_gpu_ms\":"<<setup_gpu<<",\"aggregate_table_bytes\":"<<table_bytes*roles<<",\"client_to_server_bytes\":"<<upload<<",\"server_to_client_bytes\":"<<download<<",\"coordinator_peak_rss_bytes\":"<<usage.ru_maxrss*1024<<",\"physical_dram_bytes\":null,\"energy_joules\":null,\"private\":"<<(visible?"false":"true")<<",\"placement\":\"two physical resident tables, sequential logical operators on one GPU; latency is measured sequential completion\",\"transport\":\"in-process host/device transfer; protocol bytes exclude network framing\",\"accounting\":\"CPU process phase measurements, CUDA event compute/transfers separate; no CPU+GPU sum; fresh keys every batch, every answer verified\"}";
    return 0;
  } catch(const std::exception& e){std::cerr<<e.what()<<"\n";return 1;}
}
