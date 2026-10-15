#!/usr/bin/env python3
"""Capture pinned mc historical selection against a disposable loopback peer."""
import argparse, hashlib
from pathlib import Path
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--mc', required=True)
parser.add_argument('--output', type=Path, required=True)
settings=parser.parse_args()
MC_SHA256='a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2'
assert hashlib.sha256(Path(settings.mc).read_bytes()).hexdigest()==MC_SHA256
import http.server,threading,subprocess,tempfile,os,json,urllib.parse
VERSIONS=[('top.txt','future','2026-10-16T00:00:00Z'),('top.txt','equal','2026-10-15T00:00:00Z'),('top.txt','older','2026-10-14T00:00:00Z'),('a/leaf','a-old','2026-10-14T00:00:00Z'),('gone','gone-old','2026-10-13T00:00:00Z')]
class Peer(http.server.BaseHTTPRequestHandler):
 def log_message(self,*a):pass
 def send(self,status,body=b'',head=False,fields={}):
  self.send_response(status)
  for k,v in {'Content-Length':str(len(body)),'Last-Modified':'Wed, 14 Oct 2026 00:00:00 GMT','ETag':'"e"',**fields}.items():self.send_header(k,v)
  self.end_headers()
  if not head:self.wfile.write(body)
 def do_HEAD(self):
  self.server.requests.append(['HEAD',self.path]);q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query);v=q.get('versionId',['older'])[0]
  self.send(200,v.encode(),True,{'x-amz-version-id':v})
 def do_GET(self):
  self.server.requests.append(['GET',self.path]);u=urllib.parse.urlparse(self.path);q=urllib.parse.parse_qs(u.query,keep_blank_values=True)
  if 'location' in q:self.send(200,b'<LocationConstraint/>');return
  if 'versions' in q:
   prefix=q.get('prefix',[''])[0];rows=''
   for key,version,stamp in VERSIONS:
    if key.startswith(prefix):rows+=f'<Version><Key>{key}</Key><VersionId>{version}</VersionId><IsLatest>{str(version=="future").lower()}</IsLatest><LastModified>{stamp}</LastModified><ETag>"e"</ETag><Size>{len(version)}</Size><StorageClass>STANDARD</StorageClass></Version>'
   if 'gone'.startswith(prefix):rows+='<DeleteMarker><Key>gone</Key><VersionId>deleted</VersionId><IsLatest>true</IsLatest><LastModified>2026-10-14T12:00:00Z</LastModified></DeleteMarker>'
   self.send(200,('<ListVersionsResult><Name>bucket</Name><IsTruncated>false</IsTruncated>'+rows+'</ListVersionsResult>').encode());return
  v=q.get('versionId',['current'])[0];self.send(200,v.encode(),fields={'x-amz-version-id':v})
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Peer);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start();results=[]
cutoff='2026-10-15T00:00:00Z'
commands=[['cat','--rewind',cutoff,'fixture/bucket/top.txt'],['cat','--rewind','2026-10-14T00:00:00Z','fixture/bucket/top.txt'],['head','--rewind',cutoff,'fixture/bucket/top.txt'],['ls','--rewind',cutoff,'fixture/bucket/'],['ls','--versions','--rewind',cutoff,'fixture/bucket/'],['stat','--rewind',cutoff,'fixture/bucket/top.txt'],['du','--rewind',cutoff,'fixture/bucket/'],['du','--versions','--rewind',cutoff,'fixture/bucket/'],['tree','--rewind',cutoff,'fixture/bucket/']]
try:
 with tempfile.TemporaryDirectory(prefix='ess-mc-rewind-') as config:
  env=dict({k:v for k,v in os.environ.items() if not k.startswith(('MC_','AWS_','ESS_')) and 'proxy' not in k.lower()},TZ='Asia/Shanghai',NO_PROXY='*',PAGER='cat',MC_HOST_fixture='http://fixture:fixture@127.0.0.1:'+str(server.server_port),MC_NO_COLOR='1')
  for args in commands:
   server.requests=[]
   p=subprocess.run([settings.mc,'--config-dir',config,'--json',*args],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=15)
   results.append({'args':args,'exit':p.returncode,'stdout':p.stdout.decode(errors='replace'),'stderr':p.stderr.decode(errors='replace'),'requests':server.requests})
finally:server.shutdown();server.server_close();thread.join(timeout=2)
settings.output.write_text(json.dumps({'mc_commit':'77f82e18b5401a65958f1619df6ebb994634bd88','mc_sha256':MC_SHA256,'mc_version':'DEVELOPMENT.GOGET','runtime':'go1.26.6 linux/amd64','timezone':'Asia/Shanghai','cases':results},indent=2)+'\n')
for r in results:print(json.dumps(r))
