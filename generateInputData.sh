#!/bin/bash
if [[ $# -eq 0 ]] ; then
    echo 'You should specify output file!'
    exit 1
fi

# set number of metrics
metric_num=3
# set maximum value
max_val=500

rm -rf input
mkdir input

for i in {1..200}
	do
		res_str="$((RANDOM % $metric_num)), $(date +%s%N | cut -b1-13), $((RANDOM % $max_val))"
		echo $res_str >> input/$1.1
	done

for i in {1..50}
	do
		res_str="$((RANDOM % $metric_num)), $(date +%s%N | cut -b1-13), $((RANDOM % $max_val))"
		echo $res_str >> input/$1.2
	done

for i in {1..50}
	do
		res_str="$((RANDOM % $metric_num)), $(date +%s%N | cut -b1-13), $((RANDOM % $max_val))"
		echo $res_str >> input/$1.3
	done
