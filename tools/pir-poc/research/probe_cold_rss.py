"""Check whether ru_maxrss retains a publisher's pre-exec high-water mark."""
import json
import multiprocessing as mp
from pathlib import Path
import resource


def probe(pipe):
    status={line.split(':')[0]:line.split(':',1)[1].strip() for line in Path('/proc/self/status').read_text().splitlines() if ':' in line}
    pipe.send(dict(rusage_peak_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
                   current_image_peak_bytes=int(status['VmHWM'].split()[0])*1024,
                   current_image_rss_bytes=int(status['VmRSS'].split()[0])*1024))
    pipe.close()


if __name__=='__main__':
    allocation=bytearray(128<<20)
    parent,child=mp.get_context('spawn').Pipe()
    process=mp.get_context('spawn').Process(target=probe,args=(child,));process.start();child.close()
    print(json.dumps(parent.recv()));process.join();parent.close()
