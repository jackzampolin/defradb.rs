"""Native store endpoint with the same API as the metered Python Store."""
import json
import subprocess


def encode(value):
    if isinstance(value,bytes):return {'bytes':value.hex()}
    raise TypeError(type(value).__name__)


def decode(value):
    return bytes.fromhex(value['bytes']) if set(value)=={'bytes'} else value


class NativeEndpoint:
    def __init__(self,binary,role):
        self.process=subprocess.Popen([str(binary)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
        self.role=role;self.sent=self.received=self.calls=0;self.phases=[];self.report=None
    def call(self,command,value=None):
        self.calls+=1
        line=json.dumps(dict(command=command,value=value),default=encode,separators=(',',':'))+'\n'
        self.sent+=len(line.encode());self.process.stdin.write(line);self.process.stdin.flush()
        line=self.process.stdout.readline();self.received+=len(line.encode())
        if not line:raise RuntimeError(self.process.stderr.read())
        response=json.loads(line,object_hook=decode);self.last=response
        self.phases.append(dict(phase=command,cpu_ms=response['cpu_ms']))
        return response['value']
    def close(self):
        if self.report:return self.report
        completed=self.call('close');self.process.wait(timeout=30)
        if self.process.returncode:raise RuntimeError(self.process.stderr.read())
        for pipe in (self.process.stdin,self.process.stdout,self.process.stderr):pipe.close()
        self.report=dict(role=self.role,pid=self.process.pid,phases=completed if isinstance(completed,list) else self.phases[:-1],
            process_cpu_ms=self.last['process_cpu_ms'],peak_rss_bytes=self.last['peak_rss_bytes'],
            received_bytes=self.sent,sent_bytes=self.received,peer_sent_bytes=0)
        return self.report
