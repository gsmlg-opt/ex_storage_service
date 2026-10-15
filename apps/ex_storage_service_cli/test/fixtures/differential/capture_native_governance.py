#!/usr/bin/env python3
"""Capture pinned mc portable governance requests against a disposable S3 peer."""
import argparse, hashlib
from pathlib import Path
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--mc', required=True)
parser.add_argument('--output', type=Path, required=True)
settings=parser.parse_args()
MC_SHA256='a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2'
assert hashlib.sha256(Path(settings.mc).read_bytes()).hexdigest()==MC_SHA256
import http.server, threading, subprocess, tempfile, os, json, urllib.parse
class Peer(http.server.BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def record(self):
  body=self.rfile.read(int(self.headers.get('Content-Length','0')))
  self.server.requests.append({'method':self.command,'target':self.path,'body':body.decode(),'bypass':self.headers.get('x-amz-bypass-governance-retention')})
 def send(self,body=b'',head=False):
  self.send_response(200)
  for key,value in {'Content-Length':str(len(body)),'ETag':'"e"','Last-Modified':'Wed, 14 Oct 2026 00:00:00 GMT','x-amz-version-id':'v1'}.items():self.send_header(key,value)
  self.end_headers()
  if not head:self.wfile.write(body)
 def do_HEAD(self):self.record();self.send(b'hello',True)
 def do_PUT(self):self.record();self.send()
 def do_GET(self):
  self.record();q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query,keep_blank_values=True)
  if 'location' in q:self.send(b'<LocationConstraint/>')
  elif 'object-lock' in q:self.send(b'<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>30</Days></DefaultRetention></Rule></ObjectLockConfiguration>')
  elif 'legal-hold' in q:self.send(b'<LegalHold><Status>ON</Status></LegalHold>')
  elif 'retention' in q:self.send(b'<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>')
  elif 'versions' in q:
   rows=''.join(f'<Version><Key>{key}</Key><VersionId>{version}</VersionId><Size>5</Size><ETag>"e"</ETag><LastModified>{stamp}</LastModified><IsLatest>{latest}</IsLatest></Version>' for key,version,stamp,latest in [('key','v2','2026-10-16T00:00:00Z','true'),('key','v1','2026-10-14T00:00:00Z','false'),('prefix/a','a1','2026-10-14T00:00:00Z','true')])
   self.send(('<ListVersionsResult><IsTruncated>false</IsTruncated>'+rows+'</ListVersionsResult>').encode())
  else:self.send(b'<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>prefix/a</Key><Size>5</Size><ETag>"e"</ETag><LastModified>2026-10-14T00:00:00Z</LastModified></Contents></ListBucketResult>')
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Peer);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start();results=[]
commands=[['legalhold','set','fixture/bucket/key'],['legalhold','clear','--vid','v1','fixture/bucket/key'],['legalhold','info','fixture/bucket/key'],['legalhold','set','--rewind','2026-10-15T00:00:00Z','fixture/bucket/key'],['legalhold','set','--versions','--rewind','2026-10-15T00:00:00Z','fixture/bucket/key'],['retention','set','governance','30d','fixture/bucket/key'],['retention','set','--bypass','compliance','1y','fixture/bucket/key'],['retention','clear','fixture/bucket/key'],['retention','info','--vid','v1','fixture/bucket/key'],['retention','set','--default','governance','30d','fixture/bucket/'],['retention','clear','--default','fixture/bucket/'],['retention','info','--default','fixture/bucket/'],['retention','set','governance','0d','fixture/bucket/key'],['retention','set','governance','1D','fixture/bucket/key']]
try:
 with tempfile.TemporaryDirectory(prefix='ess-mc-governance-') as config:
  env={k:v for k,v in os.environ.items() if not k.startswith(('MC_','AWS_','ESS_')) and 'proxy' not in k.lower()};env.update(MC_HOST_fixture=f'http://access:secret@127.0.0.1:{server.server_port}',NO_PROXY='*',PAGER='cat')
  for args in commands:
   server.requests=[];p=subprocess.run([settings.mc,'-C',config,'--json',*args],env=env,capture_output=True,text=True,timeout=15)
   results.append({'args':args,'exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr,'requests':server.requests})
finally:server.shutdown();server.server_close();thread.join(timeout=2)
settings.output.write_text(json.dumps({'mc_commit':'77f82e18b5401a65958f1619df6ebb994634bd88','mc_sha256':MC_SHA256,'mc_version':'DEVELOPMENT.GOGET','runtime':'go1.26.6 linux/amd64','cases':results},indent=2)+'\n')
for result in results:print(json.dumps(result))
