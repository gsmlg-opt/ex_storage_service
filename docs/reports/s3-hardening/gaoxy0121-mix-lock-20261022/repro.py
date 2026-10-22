import os,sys,pathlib,subprocess,time,json,struct,socket,fcntl,signal
root=pathlib.Path(__file__).parent
host_ns=os.environ['ESS_PROBE_HOST_NET_NS']
actual_ns=os.readlink('/proc/self/ns/net')
assert actual_ns != host_ns, (actual_ns,host_ns)
print(json.dumps({'host_net_ns':host_ns,'private_net_ns':actual_ns}),flush=True)
# All sysctl writes below occur only after asserting a distinct network namespace.
pathlib.Path('/proc/sys/net/ipv4/ip_local_port_range').write_text('32768 32769')
pathlib.Path('/proc/sys/net/ipv4/ip_local_reserved_ports').write_text('')
sock=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
req=struct.pack('16sh',b'lo',0x49);fcntl.ioctl(sock,0x8914,req);sock.close()
env=dict(os.environ,TMPDIR=str(root/'runtime-tmp-two-ports'),ERL_FLAGS='+S 2:2 +SDcpu 1 +SDio 1')
pathlib.Path(env['TMPDIR']).mkdir(exist_ok=True)
out=open(root/'worker.log','w')
worker=subprocess.Popen(['/tmp/ess-elixir-1.19-otp28-format/bin/elixir',str(root/'worker.exs')],env=env,stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
proof={'worker_os_pid':worker.pid}
try:
 deadline=time.monotonic()+12
 while time.monotonic()<deadline and worker.poll() is None:
  if 'TAKEN' in (root/'worker.log').read_text():break
  time.sleep(.05)
 proof['log']=(root/'worker.log').read_text()
 files=list((root/'runtime-tmp-two-ports').glob('mix_lock_*/*/lock_0'))
 if files and worker.poll() is None:
  lock=files[0];data=lock.read_bytes();off=1+data[0]*37;port=int.from_bytes(data[off:off+4],'big');padding=data[off+4];pid=data[off+5+padding:off+37].decode()
  proof.update(lock_file=str(lock),lock_port=port,lock_os_pid=pid,lock_inode=lock.stat().st_ino,lock_nlink=lock.stat().st_nlink)
  pf=lock.parent/f'port_{port}';proof['port_file_same_inode']=pf.exists() and pf.stat().st_ino==lock.stat().st_ino
  rows=[]
  for line in pathlib.Path('/proc/net/tcp').read_text().splitlines()[1:]:
   cols=line.split();local,remote=cols[1:3]
   if int(local.split(':')[1],16)==port or int(remote.split(':')[1],16)==port:rows.append({'local':local,'remote':remote,'state':cols[3],'inode':cols[9]})
  owned={os.readlink(f) for f in pathlib.Path(f'/proc/{worker.pid}/fd').iterdir() if f.exists()}
  for row in rows:row['worker_owned']=f"socket:[{row['inode']}]" in owned
  proof['socket_rows']=rows
  proof['self_deadlock_proven']=pid==str(worker.pid) and sum(r['state']=='0A' and r['worker_owned'] for r in rows)==1 and sum(r['state']=='01' and r['worker_owned'] for r in rows)==2 and 'TAKEN' in proof['log'] and worker.poll() is None
finally:
 if worker.poll() is None:os.killpg(worker.pid,signal.SIGTERM)
 try:worker.wait(timeout=3)
 except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=3)
 out.close();proof['worker_exit_code']=worker.returncode;proof['worker_stopped']=not pathlib.Path(f'/proc/{worker.pid}').exists()
 (root/'proof.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps(proof,indent=2),flush=True)
