#!/bin/bash
# Build and run all benchmark tests
go build -o agilepool_test .
./agilepool_test -T fixed --task-base 500 -U immediate -t 500000 -w 20000 -i 1 -f csv
./agilepool_test -T uniform --task-base 500 --task-extra 50 -U immediate -t 500000 -w 20000 -i 1 -f csv
./agilepool_test -T normal --task-mean 500 --task-sigma 50 -U immediate -t 500000 -w 20000 -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U linear --submit-interval 5 --submit-jitter 3 -t 20000 -w 20000 -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U poisson --submit-mean-interval 10 -t 10000 -w 20000 -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U phased -P "0,10,500;10,10,5000;20,10,500" --submit-shards 10 -w 20000 -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U immediate -t 200000 -w 10000 -c linkedlist -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U immediate -t 200000 -w 10000 -c minheap -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U immediate -t 200000 -w 10000 -c slice -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U immediate -t 200000 -w 10000 -c ringqueue -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U immediate -t 200000 -w 10000 -m block -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U immediate -t 200000 -w 10000 -m nonblock -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U immediate -t 20000 -w 100 -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U immediate -t 50000 -w 500 -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U immediate -t 100000 -w 2000 -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U immediate -t 200000 -w 10000 -i 1 -f csv
./agilepool_test -T fixed --task-base 100 -U immediate -t 200000 -w 10000 -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U immediate -t 200000 -w 10000 -i 1 -f csv
./agilepool_test -T fixed --task-base 2000 -U immediate -t 50000 -w 10000 -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U immediate -t 500000 -w 20000 --cpuprofile --memprofile -i 1 -f csv

# Complex orchestration tests
./agilepool_test -T fixed --task-base 500 -U phased -P "0,30,1000" --submit-shards 5 -w 10000 -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U phased -P "0,3,200;3,3,5000;6,3,200;9,3,5000" --submit-shards 8 -w 15000 -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U phased -P "0,10,2000;10,10,500" --submit-shards 5 -w 10000 -m nonblock -i 1 -f csv
./agilepool_test -T normal --task-mean 500 --task-sigma 100 -U poisson --submit-mean-interval 8 -t 15000 -w 20000 -i 1 -f csv
./agilepool_test -T uniform --task-base 500 --task-extra 100 -U linear --submit-interval 10 --submit-jitter 5 -t 10000 -w 10000 -m nonblock -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U constant --submit-interval 8 -t 15000 -w 20000 -i 1 -f csv
./agilepool_test -T fixed --task-base 1 -U immediate -t 1000000 -w 5000 -i 1 -f csv
./agilepool_test -T fixed --task-base 3000 -U immediate -t 5000 -w 500 -i 1 -f csv
./agilepool_test -T fixed --task-base 500 -U immediate -t 200000 -w 10000 -i 1 -f csv -e 3
./agilepool_test -T uniform --task-base 500 --task-extra 100 -U linear --submit-interval 15 --submit-jitter 5 -t 8000 -w 10000 -c minheap -m nonblock -i 1 -f json -e 5
