#!/bin/bash
set -x

user="centos"
ips=($(awk '{print $1}' /home/star/scripts/scale/ips_all.txt))
#TODO@luoyu: read from ips.txt
# ips=(10.77.70.143 10.77.70.145 10.77.70.252 10.77.70.253) 
# 10.77.110.147
nodes=$(( ${#ips[@]} - 2 ))
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
        ssh -i ~/.ssh/zzh_cloud $user@${ips[id]} "ps aux | grep bench_ycsb | awk '{print \$2}' | xargs sudo kill -9"
        ssh  -i ~/.ssh/zzh_cloud $user@${ips[id]} "rm -rf /core*"
    done 
bash /home/star/scripts/remove.sh # ps aux | grep bench_ycsb | awk '{print $2}' | xargs kill -9
