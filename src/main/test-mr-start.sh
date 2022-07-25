#!/usr/bin/env bash

# usage: ./test-mr-start.sh coordinator | worker

if [[ "$1" == "coordinator" ]]
then
  go build -race -buildmode=plugin ../mrapps/wc.go
  rm -rf mr-*
  go run -race mrcoordinator.go pg-*.txt
elif [[ "$1" == "worker" ]]
then
  go run -race mrworker.go wc.so
elif [[ "$1" == "cDebug" ]]
then
  go build -race -buildmode=plugin -gcflags="all=-N -l" ../mrapps/wc.go
  rm -rf mr-*
  go run -race mrcoordinator.go pg-*.txt
elif [[ "$1" == "wDebug" ]]
then
  go run -race -gcflags="all=-N -l" mrworker.go wc.so
fi