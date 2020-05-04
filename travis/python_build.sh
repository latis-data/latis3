#!/usr/bin/env bash

pyenv global 3.6
pip -q install -U pip
pip -q install -r python/requirements/requirements.txt
pip -q install -r python/requirements/jep.txt
ls /opt/python/3.6
cp /opt/python/3.6/lib/python3.6/site-packages/jep/jep.cpython-36m-x86_64-linux-gnu.so /home/travis/build/latis-data/latis3/python/lib/jep.cpython-36m-x86_64-linux-gnu.so
