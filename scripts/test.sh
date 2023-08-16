#!/bin/bash
set -x

user="centos"
#TODO@luoyu: read from ips.txt
ips=(10.77.70.116 10.77.70.117 10.77.70.252 10.77.70.253) 
# 10.77.110.147
nodes=3 # replace with the size of ips
port=20010
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
python3 /home/star/scripts/distribute_script.py $port /home/star/scripts/$workload

for id in $(seq 0 $nodes)
#TODO@luoyu: if the process has not finished yet, kill it before copy
    do
        ssh -i ~/.ssh/zzh_cloud $user@${ips[id]} "ps aux | grep zqs_laji | awk '{print \$2}' | xargs sudo kill -9"
    done 


# 



# run.sh
for id in $(seq 0 $nodes)
    do
        # ssh $user@${ips[id]} "cd /data/zhanhao; ./run.sh &"
        ssh -i ~/.ssh/zzh_cloud $user@${ips[id]} "docker exec zqs_0 bash -c \"bash /home/star/run.sh\" & "
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
    done 


# collect result
timestamp=`date "+%Y%m%d%H%M%S"`
resultDir="result-${workload}-${timestamp}"

mkdir -p /home/star/data/c/$resultDir

bash /home/star/s_get_result.bash



echo "${ips[id]}"
cp -r /home/star/data/commit /home/star/data/c/$resultDir/