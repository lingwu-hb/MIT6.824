#!/usr/bin/env bash

# usage: ./test-mr-start.sh coordinator | worker

if [[ "$1" == "coordinator" ]]
then
  go build -race -buildmode=plugin ../mrapps/wc.go
  rm -f mr-*
  go run -race mrcoordinator.go pg-*.txt
elif [[ "$1" == "worker" ]]
then
  go run -race mrworker.go wc.so
fi