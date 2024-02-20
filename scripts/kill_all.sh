#!/bin/bash
set -x

user="centos"
#TODO@luoyu: read from ips.txt
ips=(10.77.70.246 10.77.70.247 10.77.70.248 10.77.70.117) 
# 10.77.110.147
nodes=3 # replace with the size of ips
port=22010
workload=$1
run_time=$2
# skew_ratio/star/aws_star_10.py

# scp exec bin
#for id in $(seq 0 $nodes)
#    do 
#        echo "${ips[id]}"
#        scp ./bench_ycsb $user@${ips[id]}:/home/lyu/lefr/bench_ycsb
#    done

# create run.sh in nodes
#cd scripts
# python3 /home/star/scripts/distribute_script.py $port /home/star/scripts/$workload

for id in $(seq 0 $nodes)
#TODO@luoyu: if the process has not finished yet, kill it before copy
    do
        ssh -i ~/.ssh/zzh_cloud $user@${ips[id]} "ps aux | grep zqs_laji | awk '{print \$2}' | xargs sudo kill -9"
        ssh  -i ~/.ssh/zzh_cloud $user@${ips[id]} "rm -rf /core*"
    done 
bash /home/star/scripts/remove.sh # ps aux | grep zqs_laji | awk '{print $2}' | xargs kill -9
