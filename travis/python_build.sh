#!/usr/bin/env bash

pyenv global 3.6
if [[ -d $HOME/local/lib/site-packages ]] 
then
  cp -r $HOME/local/lib/site-packages /opt/python/3.6/lib/python3.6
else
  pip -q install -U pip
  pip -q install -r python/requirements/requirements.txt
  # install JEP separately to ensure it is built with numpy support 
  pip -q install -r python/requirements/jep.txt
  cp -r /opt/python/3.6/lib/python3.6/site-packages $HOME/local/lib
fi
cp /opt/python/3.6/lib/python3.6/site-packages/jep/jep.cpython-36m-x86_64-linux-gnu.so /home/travis/build/latis-data/latis3/python/lib/jep.cpython-36m-x86_64-linux-gnu.so
