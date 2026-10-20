import datetime, hashlib, importlib.util, json, os, pathlib, signal, subprocess, sys, time
repo = pathlib.Path("/home/gao/Workspace/gsmlg-opt/ex_storage_service")
scratch = pathlib.Path(__file__).resolve().parent
python = "/tmp/ess-s3-hardening-python/bin/python"
arm = sys.argv[1]
assert arm in ("baseline", "nvme")
label = "closeout002-c4-observer-diag-" + arm + "-r2"
output = repo / "docs/reports/s3-hardening" / label
assert not output.exists()
output.mkdir(mode=0o700)
trace = scratch / (arm + "-r2-traces")
trace.mkdir(mode=0o700)
fixture = "/tmp/ess-hardening-resource-" + label
base = [python, str(scratch / "launcher.py"), "e2e/s3_hardening_resources.py", str(scratch / "observer-python"), "--root", fixture, "--evidence", str(output / "workload"), "--profile", "upstream"]
env = dict(os.environ, PAGER="cat", MIX_ENV="prod", ERL_FLAGS="+S 4:4", ESS_HARDENING_PHASE2="true", ESS_HARDENING_FAILURES="true", E2E_DIAG_TRACE_DIR=str(trace))
command = base
if arm == "nvme":
    private = scratch / "nvme-private-tmp"
    private.mkdir(mode=0o700)
    command = ["bwrap", "--die-with-parent", "--new-session", "--ro-bind", "/", "/", "--bind", str(repo / "_build"), str(repo / "_build"), "--bind", str(repo / "deps"), str(repo / "deps"), "--bind", str(repo / "docs/reports"), str(repo / "docs/reports"), "--tmpfs", "/tmp", "--ro-bind", "/tmp/ess-s3-hardening-python", "/tmp/ess-s3-hardening-python", "--ro-bind", "/tmp/ess-s3-hardening-tools", "/tmp/ess-s3-hardening-tools", "--bind", str(private), "/tmp/ess-hardening-resource-nvme-parent", "--bind", str(trace), str(trace), "--chdir", str(repo)] + base
    root_index = command.index("--root") + 1
    command[root_index] = "/tmp/ess-hardening-resource-nvme-parent/run"
    fixture = command[root_index]
spec = importlib.util.spec_from_file_location("acceptance", repo / "e2e/hardening_acceptance.py")
acceptance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acceptance)
source = acceptance.source_fingerprint()
record = dict(kind="intrusive-observer-diagnostic-only", acceptance=False, profile="upstream", required_groups=8, candidate_sha=acceptance.sha(), source_sha256=source, arm=arm, command=command, environment={k:env.get(k) for k in ["PAGER","MIX_ENV","ERL_FLAGS","ESS_HARDENING_PHASE2","ESS_HARDENING_FAILURES","E2E_DIAG_TRACE_DIR"]}, inputs={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [pathlib.Path(__file__), scratch / "launcher.py", scratch / "observer-python"]}, started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), monotonic_start_ns=time.monotonic_ns(), observation="strace summary overhead and a bounded1200-row50ms thread monitor; not acceptance evidence")
receipt = output / "diagnostic.json"
receipt.write_text(json.dumps(record, indent=2)+"\n")
with (output / "command.log").open("wb") as log, (output / "thread-scheduling.jsonl").open("w") as monitor:
    child = subprocess.Popen(command, cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    print("diagnostic child", child.pid, "arm", arm, flush=True)
    start = time.monotonic()
    count = 0
    deadline = None
    try:
        while child.poll() is None:
            if time.monotonic()-start > 1800:
                raise TimeoutError("bounded diagnostic timeout")
            result = output / "workload/results.json"
            active = {}
            if result.exists():
                try: active = json.loads(result.read_text()).get("active_group") or {}
                except (ValueError, OSError): pass
            if active == {"name":"gateway-paced-upstream-put","size":1073741824,"concurrency":4} and count < 1200:
                if deadline is None: deadline = time.monotonic_ns()
                actual_start = time.monotonic_ns()
                rows = []
                observed_root = pathlib.Path(fixture) if arm=="baseline" else scratch / "nvme-private-tmp/run"
                for identity in observed_root.glob("observer-*.json"):
                    if identity.name == "observer-stop-verification.json": continue
                    expected = json.loads(identity.read_text())["observer"]
                    pid = expected["pid"]
                    task = pathlib.Path(f"/proc/{pid}/task/{pid}")
                    try:
                        fields = (task / "stat").read_text().rsplit(")",1)[1].split()
                        if int(fields[19]) != expected["start_ticks"]: raise ValueError("PID/start mismatch")
                        status = (task / "status").read_text().splitlines()
                        rows.append(dict(pid=pid,start_ticks=int(fields[19]),state=fields[0],user_ticks=int(fields[11]),system_ticks=int(fields[12]),schedstat=(task / "schedstat").read_text().strip(),context_switches=[s for s in status if "ctxt_switches" in s]))
                    except (FileNotFoundError, ProcessLookupError): pass
                monitor.write(json.dumps(dict(scheduled_monotonic_ns=deadline,actual_start_ns=actual_start,end_ns=time.monotonic_ns(),hz=os.sysconf("SC_CLK_TCK"),threads=rows))+"\n")
                monitor.flush()
                count += 1
                deadline += 50_000_000
                time.sleep(max(0,(deadline-time.monotonic_ns())/1e9))
            else: time.sleep(.2)
        code = child.wait()
    except BaseException:
        os.killpg(child.pid,signal.SIGTERM)
        try: child.wait(timeout=45)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=10)
        raise
record.update(exit_status=code, ending_sha=acceptance.sha(), source_after=acceptance.source_fingerprint(), finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),monotonic_end_ns=time.monotonic_ns(),monitor_rows=count,trace_files=[dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),bytes=p.stat().st_size) for p in trace.glob("*.txt")])
receipt.write_text(json.dumps(record,indent=2)+"\n")
print("diagnostic exit", code, flush=True)
sys.exit(code)
