#!/usr/bin/env bash

pyenv global 3.6
wget https://bootstrap.pypa.io/get-pip.py
python3.6 get-pip.py
python3.6 -m pip -q install -r python/requirements/requirements.txt
python3.6 -m pip -q install jep
cp /opt/python/3.6.10/lib/python3.6/site-packages/jep/jep.cpython-36m-x86_64-linux-gnu.so /home/travis/build/latis-data/latis3/python/lib/jep.cpython-36m-x86_64-linux-gnu.so
