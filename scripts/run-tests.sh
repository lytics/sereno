#!/bin/bash
set -ev


go test -race -cpu 2,4 -v -timeout 5m ./... 2>&1 | grep -v 'etcdserver:' | grep -v 'raft:'



