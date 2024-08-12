#!/home/jess/.venv/bin/python
# -*- coding: utf-8 -*-
import re
import sys
from grove.entrypoints.local_process import entrypoint
if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw|\.exe)?$', '', sys.argv[0])
    sys.exit(entrypoint())
