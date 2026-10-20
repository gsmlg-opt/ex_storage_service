import collections,copy,datetime,hashlib,json,pathlib,re
repo=pathlib.Path("/home/gao/Workspace/gsmlg-opt/ex_storage_service")
base=repo/"docs/reports/s3-hardening/closeout-002/candidate-c5"
ledger=repo/"docs/plans/s3-hardening-results.json"
d=json.loads(ledger.read_text());freeze=json.loads((base/"freeze.json").read_text())
assert d["tested_sha"]==freeze["candidate_sha"] and d["source_sha256"]==freeze["source_sha256"]
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def relative(p):return str(p.relative_to(repo))
def safe_path(value):
 p=(repo/value).resolve()
 assert p.is_relative_to(repo) and p.is_file() and p.stat().st_size,p
 return p
def resolve_pointer(value,pointer):
 assert pointer.startswith('/'),pointer
 for token in pointer[1:].split('/'):
  token=token.replace('~1','/').replace('~0','~')
  value=value[int(token)] if isinstance(value,list) else value[token]
 return value
def normalized(item):
 e=copy.deepcopy(item)
 if "receipt" in e:e["path"]=e["receipt"]
 if "path" not in e and "source" in e:
  match=re.fullmatch(r'(.*?):([0-9]+)(?:-([0-9]+))?',e['source'])
  e['path']=match.group(1) if match else e['source']
  e['evidence_kind']='source_semantics; execution credit requires separate current-candidate receipt'
  if match:
   lines=safe_path(e['path']).read_text().splitlines()
   assert 1<=int(match.group(2))<=int(match.group(3) or match.group(2))<=len(lines),e['source']
 p=safe_path(e["path"]);e['path']=relative(p)
 if e.get("sha256"):assert digest(p)==e["sha256"],p
 else:e["sha256"]=digest(p)
 if e.get('json_pointer'):resolve_pointer(json.loads(p.read_text()),e['json_pointer'])
 return e
profiles={name:json.loads((base/(name+".json")).read_text()) for name in ["correctness","standalone","gateway","cluster","index","resources","faults","crashes","resilience","restore","release"]}
runner=json.loads((repo/"docs/reports/s3-hardening/closeout002-c5-nvme-r2-resources/results.json").read_text())
toolchain={"elixir":"1.18.5","otp":"28","python":runner["runner"]["python"],"boto3":runner["versions"]["boto3"],"botocore":runner["versions"]["botocore"],"aws_cli":"2.34.24","mc":"RELEASE.2025-08-13T08-35-41Z","dependencies":runner["runner"]["dependencies"]}
rows={r["id"]:r for r in d["scenarios"]}
assert len(d['scenarios'])==len(rows)==38 and set(rows)=={f'A{i:02}' for i in range(1,39)}
supplement=base/'nvme-resources-r2'
supplement_record=json.loads((supplement/'resources.json').read_text())
supplement_exit=json.loads((supplement/'authoritative-exit.json').read_text())
assert supplement_record['starting_sha']==freeze['candidate_sha'] and supplement_record['source_sha256']==freeze['source_sha256']
assert supplement_record['status']=='blocked' and supplement_exit['exit_status']==1
supplement_evidence=dict(path=relative(supplement/'resources.json'),sha256=digest(supplement/'resources.json'),status=supplement_record['status'],actual_parent_exit_status=supplement_exit['exit_status'],commands=supplement_record['commands'],runner=runner['runner'],scope='Supplemental unchanged resource profile; original failed lane retained. Cluster20/20 and actual2GiB passed; local52/56 failed and four cases unexecuted.')
for name in ["clause-audit-a01-a27.json","clause-audit-a29-a38.json"]:
 p=base/name;aud=json.loads(p.read_text());assert aud["candidate_sha"]==freeze["candidate_sha"] and aud["source_sha256"]==freeze["source_sha256"]
 for ri,review in enumerate(aud["rows"]):
  row=rows[review["id"]];by_id={c["id"]:c for c in row["clauses"]};assert len(by_id)==len(row['clauses'])==len(review['clauses']) and set(by_id)==set(c["id"] for c in review["clauses"])
  modes=set(review.get("lanes",[]));assertions=[];row_evidence=[dict(path=relative(p),sha256=digest(p),json_pointer=f"/rows/{ri}")]
  for ci,c in enumerate(review["clauses"]):
   target=by_id[c["id"]];status=c["status"];target["status"]="passed" if status in ["passed","verified"] else "failed" if row["id"]=="A30" else "blocked"
   evidence=[normalized(e) for e in c["evidence"]]
   pointer=dict(path=relative(p),sha256=digest(p),json_pointer=f"/rows/{ri}/clauses/{ci}")
   target["evidence"]=[pointer,*evidence];target["remaining"]=c.get("remaining",[])
   if c.get("required_modes"):target["required_modes"]=c["required_modes"]
   statements=[]
   if c.get("assertion_review"):statements.append(c["assertion_review"])
   for e in evidence:
    modes.update([e["mode"]] if "mode" in e else [])
    statements.extend(e.get("verified_assertions",[]))
    if e.get('assertion'):statements.append('Source assertion mapped to separately recorded execution: '+e['assertion'])
   assert statements or target["status"]!="passed",c["id"]
   assertion=c["id"]+": "+" ".join(dict.fromkeys(statements))
   if c.get("remaining"):assertion+=" Remaining: "+"; ".join(c["remaining"])
   assertions.append(assertion);target["assertion_audit"]=statements
   for e in evidence:
    if e["path"] not in {v["path"] for v in row_evidence}:row_evidence.append(e)
  row["status"]="passed" if all(c["status"]=="passed" for c in row["clauses"]) else "failed" if row["id"]=="A30" else "blocked"
  row["evidence"]=row_evidence;row["assertion_audit"]=assertions
  commands=[];failed=[]
  for mode in sorted(modes):
   if mode not in profiles:continue
   record=profiles[mode];assert record["source_sha256"]==freeze["source_sha256"] and record["starting_sha"]==freeze["candidate_sha"]
   for i,command in enumerate(record["commands"]):
    if command["exit_status"]==0:commands.append(command["argv"])
    else:failed.append(dict(profile=mode,command_index=i,exit_status=command["exit_status"],scope="Failed parent command remains failed; only separately identified successful assertions can certify a clause."))
  row["verification"]=dict(command=commands,mode=sorted(modes),toolchain=toolchain,starting_sha=freeze["candidate_sha"],ending_sha=freeze["candidate_sha"],source_sha256=freeze["source_sha256"],exit_status=0 if row["status"]=="passed" else 1,scope="Semantic audit of actual successful current-candidate commands, with independent receipts and failed parent commands retained. This does not certify aggregate acceptance.",failed_parent_commands=failed)
  if row['id'] in ['A30','A38']:row['verification']['supplemental_runs']=[copy.deepcopy(supplement_evidence)]
r28=rows['A28']
r28['evidence']=[normalized(dict(path=relative(base/'upstream-refresh.json'))),normalized(dict(path=relative(base/'index.json')))]
r28['assertion_audit']=['Current index preflight actually exited1 before either mandatory dataset; no10k/100k,p50/p95,allocation or unrelated-growth acceptance is credited.','Concord3.1.4 incorporates#99/#100 but released native traversal/materialization repairs for#101/#102 are still unavailable.','Current complete correctness and independent A29 command passed; these narrower results do not certify the blocked complete A28 profile.']
for clause in r28['clauses']:
 clause['evidence']=copy.deepcopy(r28['evidence'])
 clause['remaining']=['Complete original10k/100k index lane after released upstream#101/#102repairs, revalidate affected invariants/migration/mutation checks, and retain actual native work,allocation,p50/p95 and setup measurements.']
 clause['assertion_audit']=['Blocked current-candidate index command is preserved; narrower executed checks and prior-source diagnostics do not certify this compound acceptance requirement.']
r28['verification']=dict(command=[c['argv'] for c in profiles['index']['commands']],mode=['index','correctness'],toolchain=toolchain,starting_sha=freeze['candidate_sha'],ending_sha=freeze['candidate_sha'],source_sha256=freeze['source_sha256'],exit_status=1,scope='Actual current index preflight blocked; independent A29 command passed but mandatory10k/100k unexecuted.',failed_parent_commands=[dict(profile='index',command_index=1,exit_status=1)])
d.update(ending_sha=freeze["candidate_sha"],scenario_counts=dict(collections.Counter(r["status"] for r in d["scenarios"])),current_source_passed_rows=[r["id"] for r in d["scenarios"] if r["status"]=="passed"],current_passed_profiles=[k for k,v in profiles.items() if v["status"]=="passed"],full_hardening_acceptance=False,current_runtime_evidence_status="Frozen C5 all-profile run terminal exit1; original failures preserved. Separate resource supplement and explicitly nonacceptance diagnostics do not replace failed lanes.")
d["current_execution_scope"]["status"]="blocked"
d["delivery_verification"]["status"]="blocked_full_acceptance_not_passed"
d["aggregate_verification"]={"status":"pending_final_verification","exit_status":None,"receipt":relative(base/"aggregate.json"),"authoritative_run_exit_status":json.loads((base/"authoritative-exit.json").read_text())["exit_status"]}
ledger.write_text(json.dumps(d,indent=2)+"\n")
clauses=[c for r in d["scenarios"] for c in r["clauses"]]
print(json.dumps(dict(rows=d["scenario_counts"],clauses=dict(collections.Counter(c["status"] for c in clauses)),passed_profiles=d["current_passed_profiles"]),indent=2))
