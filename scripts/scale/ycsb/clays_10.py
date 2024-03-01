import sys
import math
ips = [line.strip() for line in open(str(sys.argv[3]), "r")]
n = len(ips)

# print(ips)

ins = [line.split(" ")[0] for line in ips]
outs = [line.split(" ")[1] for line in ips]

id = int(sys.argv[1]) 
port = int(sys.argv[2]) 

protocols = ["CLAY-S"]
ratios = [0] # , 10, 20, 30, 40, 50, 60, 70, 80, 90, 100

def get_cmd(n, i):
  cmd = ""
  for j in range(n):
    if j > 0:
      cmd += ";"
    if id == j:
      cmd += ins[j] + ":" + str(port+i)
    else:
      cmd += outs[j] + ":" + str(port+i)
  return cmd
partition_num = int(math.ceil(12 * 11 / (len(ips) - 1) / 4)  * 4)
for protocol in protocols: 
  for i in range(len(ratios)):
    ratio = ratios[i]
    cmd = get_cmd(n, i)
    print('/home/star/bench_ycsb --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=%d --partitioner=Lion --threads=2 --batch_size=10000 --batch_flush=500   --cross_ratio=100 --lion_with_metis_init=1 --time_to_run=120   --sample_time_interval=3 --migration_only=0 --n_nop=200000 --v=5 --data_src_path_dir="/home/star/data/ycsb/ips%d/"  --read_on_replica=true  --random_router=0 --workload_time=120 --lion_self_remaster=0 --repartition_strategy=clay' % (id, cmd, protocol, partition_num, n - 1))
      
# /home/star/bench_ycsb --logtostderr=1 --id=0 --servers="10.77.70.250:10210;10.77.70.251:10211;10.77.70.248:10210;10.77.70.117:10210;10.77.110.147:10212" --protocol=Star --partition_num=12 --partitioner=hash2 --threads=4 --batch_size=10000 --batch_flush=500 --lion_with_metis_init=0 --time_to_run=60 --workload_time=60 --sample_time_interval=3 --migration_only=1 --n_nop=20000 --v=8 

# for protocol in protocols: 
#   for i in range(len(ratios)):
#     ratio = ratios[i]
#     cmd = get_cmd(n, i)
#     print('./bench_tpcc --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --partitioner=hash2 --query=mixed --neworder_dist=%d --payment_dist=%d' % (id, cmd, protocol, 12*n, ratio, ratio))   
      