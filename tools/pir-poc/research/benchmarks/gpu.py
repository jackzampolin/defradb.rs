"""Build the fresh-query GPU adapter against a pinned, copied upstream tree."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

REVISION="ce23a06af884ee54300b5bc5fd5350e445f10b0b"


def run(source,output,candidate="dense",rows=1024,row_bytes=32,queries=4,batch=1):
    if rows<256 or rows&(rows-1) or row_bytes not in (32,96,256,1024):raise ValueError("GPU dimensions unsupported")
    if subprocess.check_output(["git","-C",str(source),"rev-parse","HEAD"],text=True).strip()!=REVISION:raise ValueError("GPU-DPF revision mismatch")
    if subprocess.check_output(["git","-C",str(source),"status","--porcelain"],text=True).strip():raise ValueError("GPU source must be pristine")
    output.mkdir(parents=True,exist_ok=False)
    root=output/"source";shutil.copytree(source,root,ignore=shutil.ignore_patterns(".git"))
    header=root/"dpf_base/dpf.h";code=header.read_text()
    old='''  std::uniform_int_distribution<uint64_t> d(0, std::numeric_limits<uint64_t>::max());
  uint64_t l = d(gen);
  uint64_t r = d(gen);
  return ((uint128_t)l) << 64 | (uint128_t)r;'''
    if code.count(old)!=1 or code.count("s->codewords_1[i] = g_gen();")!=1:raise ValueError("GPU randomness patch context changed")
    code=code.replace(old,"  uint128_t value; secure_fill(&value,sizeof(value)); return value;").replace("s->codewords_1[i] = g_gen();","s->codewords_1[i] = GenerateRandomNumber(g_gen);")
    header.write_text(code)
    adapter=Path(__file__).resolve().parents[1]/"gpu_dpf_adapter"
    nvcc=shutil.which("nvcc") or "/opt/cuda-12.4/bin/nvcc"
    smi=shutil.which("nvidia-smi") or "/usr/lib/wsl/lib/nvidia-smi"
    capability=subprocess.check_output([smi,"--query-gpu=compute_cap","--format=csv,noheader"],text=True).splitlines()[0].strip().replace(".","")
    binary=output/"gpu-total-work"
    argv=[nvcc,"-O3","-std=c++17","-allow-unsupported-compiler","-ccbin","g++-13",f"-arch=sm_{capability}",f"-I{root}","-Xcompiler=-pthread","-ldl",f"-DDEFRA_LIMBS={row_bytes//16}",str(adapter/"total_work.cu"),"-o",str(binary)]
    compiler_version=subprocess.check_output([nvcc,"--version"])
    cache_key=hashlib.sha256(header.read_bytes()+b"".join(p.read_bytes() for p in sorted(adapter.glob("*.cu")))+compiler_version+f"{row_bytes}:{capability}".encode()).hexdigest()
    cache=output.parent/"gpu-build-cache"/cache_key
    if (cache/"binary.json").is_file():
        cached=json.loads((cache/"binary.json").read_text())
        if hashlib.sha256((cache/"binary").read_bytes()).hexdigest()!=cached["sha256"]:raise ValueError("GPU build cache hash mismatch")
        shutil.copy2(cache/"binary",binary)
    else:
        with (output/"build.log").open("w") as log:subprocess.run(argv,stdout=log,stderr=subprocess.STDOUT,check=True)
        cache.mkdir(parents=True,exist_ok=False);shutil.copy2(binary,cache/"binary")
        (cache/"binary.json").write_text(json.dumps(dict(sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),compiler=argv)))
    manifest=dict(upstream_revision=REVISION,compiler=argv,binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
        patched_header_sha256=hashlib.sha256(header.read_bytes()).hexdigest(),adapter_sha256={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in adapter.glob("*.cu")},
        randomness="All DPF secret random draws and Dense selectors use Linux getrandom; upstream MT parameter remains unused for secret sampling.",
        config=dict(candidate=candidate,rows=rows,row_bytes=row_bytes,queries=queries,batch=batch))
    (output/"manifest.json").write_text(json.dumps(manifest,indent=2))
    with (output/"execution.log").open("w") as log:
        subprocess.run([str(binary),candidate,str(rows),str(batch),str(queries),str(output/"result.json")],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=600)
    return json.loads((output/"result.json").read_text())
