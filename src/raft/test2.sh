#!/usr/bin/env bash

go test -run BasicAgree\|FailAgree\|FailNoAgree\|ConcurrentStarts\|Rejoin\|Backup -count 10 -timeout 2h