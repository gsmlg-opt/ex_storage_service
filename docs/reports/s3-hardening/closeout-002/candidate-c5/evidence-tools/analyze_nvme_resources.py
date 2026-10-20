import collections,datetime,hashlib,json,pathlib,subprocess
repo=pathlib.Path("/home/gao/Workspace/gsmlg-opt/ex_storage_service")
out=repo/"docs/reports/s3-hardening/closeout-002/candidate-c5/nvme-resources-r2"
terminal=json.loads((out/"authoritative-exit.json").read_text());assert "exit_status" in terminal
roots=[repo/"docs/reports/s3-hardening"/name for name in ["closeout002-c5-nvme-r2-resources","closeout002-c5-nvme-r2-resources-cluster"]]
def digest(p):
 h=hashlib.sha256()
 with p.open("rb") as f:
  for block in iter(lambda:f.read(1048576),b""):h.update(block)
 return h.hexdigest()
def observations(g):
 return list(g["nodes"].values()) if "nodes" in g else [g]
def identity(pid):
 try:
  fields=pathlib.Path(f"/proc/{pid}/stat").read_text().rsplit(")",1)[1].split()
  return dict(pid=pid,start_ticks=int(fields[19]),state=fields[0])
 except (FileNotFoundError,ProcessLookupError):return None
exports=[];identities=[];matrices=[]
for root in roots:
 for p in root.rglob("resource-export-*.json"):
  m=json.loads(p.read_text());s=p.parent/"samples.jsonl";h=hashlib.sha256();count=0;size=0;previous=None
  with s.open("rb") as f:
   f.seek(m["output_start"])
   while size<m["output_bytes"]:
    line=f.readline(m["row_byte_limit"]+1)
    assert line and line.endswith(b"\n") and len(line)<=m["row_byte_limit"]
    h.update(line);size+=len(line);row=json.loads(line)
    assert str(row["sampler_id"])==str(m["generation"])
    assert row["sequence"]==count+m["first_sequence"]
    count+=1
  assert m["status"]=="passed" and count==m["admitted_rows"]==m["persisted_rows"]
  assert size==m["output_bytes"] and h.hexdigest()==m["sha256"]
  assert count+m["first_sequence"]-1==m["last_sequence"]
  exports.append(dict(manifest=str(p.relative_to(repo)),path=str(s.relative_to(repo)),sha256=h.hexdigest(),bytes=size,verified_rows=count,generation=m["generation"],status="passed"))
 for p in root.rglob("observer-*.json"):
  d=json.loads(p.read_text())
  if not isinstance(d,dict) or "observer" not in d:continue
  for role in ["beam","observer"]:
   expected=d[role];actual=identity(expected["pid"]);stopped=actual is None or actual["start_ticks"]!=expected["start_ticks"]
   assert stopped
   identities.append(dict(source=str(p.relative_to(repo)),role=role,expected=expected,observed=actual,stopped=stopped))
 p=root/"results.json";d=json.loads(p.read_text());ident=lambda g:(g["name"],g["size"],g["concurrency"])
 required=collections.Counter(map(ident,d["required_cases"]));actual=collections.Counter(map(ident,d["groups"]))
 groups=d["groups"]
 matrices.append(dict(path=str(p.relative_to(repo)),sha256=digest(p),status=d["status"],required=sum(required.values()),executed=len(groups),failure_free=sum(not g.get("failures") for g in groups),missing=list((required-actual).elements()),unexpected=list((actual-required).elements()),failed=[{k:g.get(k) for k in ["name","size","concurrency","failures","operation_errors"]} for g in groups if g.get("failures")],maximum_probe_ms=max(c.get("maximum_probe_ns",0) for g in groups for n in observations(g) for c in n["cadence"].values())/1e6,maximum_gap_ms=max(c.get("maximum_gap_ns",0) for g in groups for n in observations(g) for c in n["cadence"].values())/1e6,per_size_concurrency=[dict(size=z,concurrency=c,groups=sum((g["size"],g["concurrency"])==(z,c) for g in groups),maximum_delta={metric:max(n["delta"].get(metric,0) for g in groups if (g["size"],g["concurrency"])==(z,c) for n in observations(g)) for metric in ["binary","total","rss","temporary","queue_max","queue_total"]}) for z,c in sorted(set((g["size"],g["concurrency"]) for g in groups))]))
namespace=terminal["runner_change"]["physical_tmp"]
staging={}
for p in (pathlib.Path(namespace)/"ess-hardening-resource-closeout002-c5-nvme-r2-resources/data/tmp").rglob("*"):
 if p.is_file():
  kind="anonymous" if p.name.startswith("ess1-a-") else "hosted-protected" if p.name.startswith("ess1-h-") else "gateway-protected" if p.name.startswith("ess1-g-") else "other"
  row=staging.setdefault(kind,dict(count=0,bytes=0));row["count"]+=1;row["bytes"]+=p.stat().st_size
names=subprocess.check_output(["epmd","-names"],text=True).splitlines()
owned_names=[n for n in names if any(v in n for v in ["ess_gateway_","ess_hardening_","ess_resource_"])]
containers=subprocess.check_output(["docker","ps","--format","{{.Names}}"],text=True).splitlines();owned_containers=[n for n in containers if n.startswith("ess-hardening-")]
assert not owned_names and not owned_containers
r=dict(schema=1,candidate_sha=terminal["candidate_sha"],source_sha256=terminal["source_sha256"],verified_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),actual_exit_status=terminal["exit_status"],method="Independent bounded streaming verification of exact generation/sequence/count/extent/hash; integrity does not certify cadence or missing cases.",exports=exports,matrices=matrices,terminal_cleanup=dict(status="passed",recorded_identities=identities,remaining_owned_erlang_names=owned_names,remaining_owned_containers=owned_containers),local_terminal_staging=staging,retention_interpretation="Classified bytes retained by stopped isolated fixture; protection and intent outcomes require service diagnosis, not proof of permanent leakage.")
(out/"resource-observation-analysis.json").write_text(json.dumps(r,indent=2)+"\n")
print(json.dumps(dict(actual_exit=terminal["exit_status"],exports=len(exports),rows=sum(x["verified_rows"] for x in exports),cleanup_identities=len(identities),matrices=[{k:m[k] for k in ["status","required","executed","failure_free","missing","failed","maximum_probe_ms","maximum_gap_ms"]} for m in matrices],staging=staging),indent=2))
