import os, runpy, sys
script, wrapper, *arguments = sys.argv[1:]
sys.path.insert(0, os.path.dirname(os.path.abspath(script)))
sys.executable = os.path.abspath(wrapper)
sys.argv = [script, *arguments]
runpy.run_path(script, run_name="__main__")
