import sys, os

cwd = "".join(reversed(os.getcwd()))
test_dir = "tset-ppa"
try:
    base_dir = "".join(reversed(cwd[cwd.index(test_dir) + len(test_dir):]))
except ValueError:
    base_dir = os.getcwd()

if base_dir not in sys.path:
    sys.path.append(base_dir)


from . import controller as crud
from dbsync import client
