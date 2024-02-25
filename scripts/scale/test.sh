#!/bin/bash
set -x

user="centos"
ips=($(awk '{print $1}' $1))
#TODO@luoyu: read from ips.txt
# ips=(10.77.70.143 10.77.70.145 10.77.70.252 10.77.70.253) 
# 10.77.110.147
nodes=$(( ${#ips[@]} - 2 ))
# replace with the size of ips
port=22010
ipstxt=$1
workload=$2
run_time=$3

python3 /home/star/scripts/scale/distribute_script.py $port /home/star/scripts/$workload $ipstxt

rm -rf /core*

for id in $(seq 0 $nodes)
#TODO@luoyu: if the process has not finished yet, kill it before copy
    do
        echo "$id ${ips[id]}"
        ssh -i ~/.ssh/zzh_cloud $user@${ips[id]} "ps aux | grep zqs_laji | awk '{print \$2}' | xargs sudo kill -9"
        ssh -i ~/.ssh/zzh_cloud $user@${ips[id]} "sudo docker exec zqs_0 bash -c \"rm -rf /core* \" & "
    done 


# # 

# docker exec zqs_0 bash -c "bash /home/star/run.sh"
# run.sh
for id in $(seq 0 $nodes)
    do
        # ssh $user@${ips[id]} "cd /data/zhanhao; ./run.sh &"
        ssh -i ~/.ssh/zzh_cloud $user@${ips[id]} "sudo docker exec zqs_0 bash -c \"bash /home/star/run.sh\" & "
        sleep 3s
    done

bash /home/star/run.sh &
sleep 3s


# wait for finishing
sleep ${run_time}s

bash /home/star/scripts/remove.sh # ps aux | grep zqs_laji | awk '{print $2}' | xargs kill -9

for id in $(seq 0 $nodes)
#TODO@luoyu: if the process has not finished yet, kill it before copy
    do
        ssh -i ~/.ssh/zzh_cloud $user@${ips[id]} "ps aux | grep zqs_laji | awk '{print \$2}' | xargs sudo kill -9"
        ssh -i ~/.ssh/zzh_cloud $user@${ips[id]} "sudo docker exec zqs_0 bash -c \"rm -rf /core* \" & "
    done 


# collect result
timestamp=`date "+%Y%m%d%H%M%S"`
nodesStr=$(( ${#ips[@]} - 1 ))
resultDir="result-${workload}-${nodesStr}-${timestamp}"

mkdir -p /home/star/data/c/$resultDir

# bash /home/star/scripts/scale/s_get_result.bash

for id in $(seq 0 $nodes)
#TODO@luoyu: if the process has not finished yet, kill it before copy
    do
        scp -i  ~/.ssh/zzh_cloud $user@${ips[id]}:/home/docker/volumes/zqs-vol/_data/star/data/commits_${id}.xls  /home/star/data/commit/
    done 

echo "${ips[id]}"
cp -r /home/star/data/commit /home/star/data/c/$resultDir/