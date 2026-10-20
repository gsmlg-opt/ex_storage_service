import datetime,hashlib,importlib.util,json,os,pathlib,signal,subprocess,sys,time,shutil
repo=pathlib.Path("/home/gao/Workspace/gsmlg-opt/ex_storage_service")
base=pathlib.Path(__file__).resolve().parent
label="closeout002-c5-copy-diag-r3"
out=repo/"docs/reports/s3-hardening"/label
assert not out.exists()
terminal=json.loads((repo/"docs/reports/s3-hardening/closeout-002/candidate-c5/nvme-resources-r2/authoritative-exit.json").read_text())
assert "exit_status" in terminal, "Heavy resource lane must finish first"
spec=importlib.util.spec_from_file_location("acceptance",repo/"e2e/hardening_acceptance.py");m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
assert m.sha()==terminal["ending_sha"] and m.source_fingerprint()==terminal["source_after"]
out.mkdir(mode=0o700)
inputs=out/"inputs";inputs.mkdir()
for name in ["run.py","workload.py","workload.diff","copy_trace.exs"]:shutil.copyfile(base/name,inputs/name)
namespace=json.loads(pathlib.Path("/fast/build/ess-closeout-c5-nvme-resources/namespace-command.json").read_text())
namespace=[str(base/"tmp") if s=="/fast/build/ess-closeout-c5-nvme-resources/tmp" else s for s in namespace]
fixture="/tmp/ess-hardening-resource-"+label
command=namespace+["/tmp/ess-s3-hardening-python/bin/python",str(base/"workload.py"),"--root",fixture,"--evidence",str(out/"workload"),"--profile","all"]
env=dict(os.environ,PAGER="cat",MIX_ENV="prod",ERL_FLAGS="+S 4:4 +SDcpu 2 +SDio 2",ESS_HARDENING_PHASE2="true",ESS_HARDENING_FAILURES="true",PYTHONPATH=str(repo/"e2e"))
assert not any(k in env for k in ["AWS_REQUEST_CHECKSUM_CALCULATION","AWS_RESPONSE_CHECKSUM_VALIDATION","E2E_DIAG_TRACE_DIR"])
r=dict(kind="bounded-gateway-copy-error-diagnostic",acceptance=False,candidate_sha=m.sha(),source_sha256=m.source_fingerprint(),command=command,started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),monotonic_start_ns=time.monotonic_ns(),inputs={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs.iterdir()},physical_tmp=str(base/"tmp"),required_cases="Unchanged original resource manifest prefix through1GiB/c4gateway-copy-to-local; exact52 identities in workload/results.json",intrusive_return_tracing=True,criteria_sizes_concurrency_auth_checksums_RFW="unchanged original52group prefix; bounded return tracing makes this diagnostic, not acceptance")
receipt=out/"diagnostic.json"
with (out/"command.log").open("wb") as log:
 child=subprocess.Popen(command,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 r.update(child_pid=child.pid);receipt.write_text(json.dumps(r,indent=2)+"\n");print("copy diagnostic child",child.pid,flush=True)
 try:code=child.wait(timeout=1800)
 except BaseException:
  os.killpg(child.pid,signal.SIGTERM)
  try:child.wait(timeout=45)
  except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=10)
  raise
observed=base/"tmp"/pathlib.Path(fixture).relative_to("/tmp")
for pattern in ["storage-errors.jsonl","copy-returns.jsonl"]:
 p=observed/pattern
 if p.exists():shutil.copyfile(p,out/pattern)
classes={}
for p in (observed/"data/tmp").rglob("*"):
 if p.is_file():
  kind=("anonymous" if p.name.startswith("ess1-a-") else "hosted-protected" if p.name.startswith("ess1-h-") else "gateway-protected" if p.name.startswith("ess1-g-") else "other")
  row=classes.setdefault(kind,dict(count=0,bytes=0));row["count"]+=1;row["bytes"]+=p.stat().st_size
r.update(exit_status=code,finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),monotonic_end_ns=time.monotonic_ns(),ending_sha=m.sha(),source_after=m.source_fingerprint(),terminal_staging=classes)
receipt.write_text(json.dumps(r,indent=2)+"\n");print("copy diagnostic actual exit",code,flush=True);sys.exit(code)
