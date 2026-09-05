"""Read-only hardware capability probes; unavailable measurements stay null."""
import glob
import os
import platform
import shutil
import subprocess
from pathlib import Path


def command(argv):
    try:
        r=subprocess.run(argv,text=True,capture_output=True,timeout=15)
        return dict(command=argv,returncode=r.returncode,stdout=r.stdout,stderr=r.stderr)
    except (OSError,subprocess.TimeoutExpired) as error:return dict(command=argv,error=str(error))


def probe():
    return dict(platform=platform.platform(),host=platform.node(),logical_cpus=os.cpu_count(),
        cpu=command(["lscpu","--json"]),memory=Path("/proc/meminfo").read_text(),
        perf=command(["perf","stat","-x",",","-e","cycles,instructions,cache-misses","--","true"]),
        powercap_paths=glob.glob("/sys/class/powercap/*/energy_uj"),
        uncore_paths=glob.glob("/sys/bus/event_source/devices/uncore*"),
        gpu=command([shutil.which("nvidia-smi") or "/usr/lib/wsl/lib/nvidia-smi","--query-gpu=name,uuid,memory.total,driver_version","--format=csv"]),
        placement="single physical host; multiple independent processes are not independent machines",
        physical_dram_bytes=None,energy_joules=None,
        counter_note="Capability discovery is not a workload measurement. Use --perf for process counters; DRAM and energy require calibrated platform counters.")


def perf_prefix(path):
    return ["perf","stat","-x",",","-o",str(path),"-e","task-clock,cycles,instructions,cache-references,cache-misses,page-faults,context-switches","--"]
