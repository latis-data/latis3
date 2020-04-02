#!/usr/bin/env bash

if [[ -f $HOME/local/lib/libnetcdf.so.18.0.0 ]]
then
  if [[ ! -f $HOME/local/lib/libnetcdf.so ]]
  then
    ln -s $HOME/local/lib/libnetcdf.so.18.0.0 $HOME/local/lib/libnetcdf.so
  fi
else
  cd $HOME
  curl -OL https://github.com/Unidata/netcdf-c/archive/v4.7.4.tar.gz
  tar -xf v4.7.4.tar.gz
  cd netcdf-c-4.7.4
  CPPFLAGS=-I/usr/include/hdf5/serial LDFLAGS=-L/usr/lib/x86_64-linux-gnu/hdf5/serial ./configure --prefix=$HOME/local
  make install
fi
