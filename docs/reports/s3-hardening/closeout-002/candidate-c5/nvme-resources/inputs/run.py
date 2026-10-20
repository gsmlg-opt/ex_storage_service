import datetime,hashlib,importlib.util,json,os,pathlib,signal,subprocess,sys,time
repo=pathlib.Path("/home/gao/Workspace/gsmlg-opt/ex_storage_service")
base=pathlib.Path(__file__).resolve().parent
out=repo/"docs/reports/s3-hardening/closeout-002/candidate-c5/nvme-resources"
out.mkdir(mode=0o700,exist_ok=False)
spec=importlib.util.spec_from_file_location("acceptance",repo/"e2e/hardening_acceptance.py");m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
freeze=json.loads((out.parent/"freeze.json").read_text());assert m.sha()==freeze["candidate_sha"] and m.source_fingerprint()==freeze["source_sha256"]
assert "exit_status" in json.loads((out.parent/"authoritative-exit.json").read_text())
assert not any(k in os.environ for k in ["AWS_REQUEST_CHECKSUM_CALCULATION","AWS_RESPONSE_CHECKSUM_VALIDATION","E2E_DIAG_TRACE_DIR"])
base_command=json.loads((base/"namespace-command.json").read_text())
command=base_command+["/tmp/ess-s3-hardening-python/bin/python","e2e/hardening_acceptance.py","run","--profile","resources","--python","/tmp/ess-s3-hardening-python/bin/python","--mc","/tmp/ess-s3-hardening-tools/mc","--label","closeout002-c5-nvme","--output",str(out.relative_to(repo))]
env=dict(os.environ,PAGER="cat",ERL_FLAGS="+S 4:4 +SDcpu 2 +SDio 2")
r=dict(kind="unchanged-full-resource-lane-on-isolated-second-nvme",acceptance=True,candidate_sha=m.sha(),source_sha256=m.source_fingerprint(),command=command,started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),monotonic_start_ns=time.monotonic_ns(),environment={"PAGER":"cat","ERL_FLAGS":env["ERL_FLAGS"],"SDKchecksum_overrides":"unset","intrusive_tracing":"absent"},runner_change={"only_change":"Private mount namespace maps /tmp onto isolated /fast directory on nvme1 ext4; original receipts on system nvme0 ext4 retained.","physical_tmp":str(base/"tmp"),"preflight":json.loads((base/"preflight.json").read_text()),"cpu_memory_limits_sizes_concurrency_thresholds_auth_checksums_RFW":"unchanged"},driver_sha256=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest())
receipt=out/"authoritative-exit.json"
with (out/"authoritative.log").open("wb") as log:
 child=subprocess.Popen(command,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 fields=pathlib.Path(f"/proc/{child.pid}/stat").read_text().rsplit(")",1)[1].split();r.update(child_pid=child.pid,child_start_ticks=int(fields[19]));receipt.write_text(json.dumps(r,indent=2)+"\n");print("isolatedNVMe resources child",child.pid,flush=True)
 def stop(signum,_frame):
  os.killpg(child.pid,signal.SIGTERM)
  try:child.wait(timeout=45)
  except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=10)
  raise SystemExit(128+signum)
 signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
 code=child.wait()
r.update(exit_status=code,finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),monotonic_end_ns=time.monotonic_ns(),ending_sha=m.sha(),source_after=m.source_fingerprint());receipt.write_text(json.dumps(r,indent=2)+"\n");print("isolatedNVMe resources actualexit",code,flush=True);sys.exit(code)
