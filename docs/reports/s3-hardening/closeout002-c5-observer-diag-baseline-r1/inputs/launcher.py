import os, runpy, sys
script, wrapper, *arguments = sys.argv[1:]
sys.path.insert(0, '/home/gao/Workspace/gsmlg-opt/ex_storage_service/e2e')
sys.executable = os.path.abspath(wrapper)
sys.argv = [script, *arguments]
runpy.run_path(script, run_name="__main__")
