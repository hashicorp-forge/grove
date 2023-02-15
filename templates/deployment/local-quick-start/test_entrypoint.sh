# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

cd app
echo 'Installing Grove...'
python3 setup.py install > /dev/null 2>&1
echo 'Installation complete. Starting Grove'
grove
