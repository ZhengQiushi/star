import sys

ips = [line.strip() for line in open("/home/star/scripts/ips.txt", "r")]
n = len(ips)

# print(ips)

ins = [line.split(" ")[0] for line in ips]
outs = [line.split(" ")[1] for line in ips]

id = int(sys.argv[1]) 
port = int(sys.argv[2]) 

protocols = ["LION-S"]
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

for protocol in protocols: 
  for i in range(len(ratios)):
    ratio = ratios[i]
    cmd = get_cmd(n, i)
    print('/home/star/bench_tpcc --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=24 --partitioner=Lion --threads=4 --neworder_dist=20 --batch_size=10000 --batch_flush=500 --lion_with_trace_log=false --data_src_path_dir="/home/star/data/tpcc/skew_0_30/" --v=7 --time_to_run=120 --workload_time=60 --sample_time_interval=3 --n_nop=0 --lion_with_metis_init=0 --migration_only=1 --read_on_replica=true --random_router=0 --skew_factor=80 --rn_nop=40000' % (id, cmd, protocol))
      
# /home/star/bench_ycsb --logtostderr=1 --id=0 --servers="10.77.70.250:10210;10.77.70.251:10211;10.77.70.248:10210;10.77.70.117:10210;10.77.110.147:10212" --protocol=Star --partition_num=12 --partitioner=hash2 --threads=4 --batch_size=10000 --batch_flush=500 --lion_with_metis_init=0 --time_to_run=60 --workload_time=60 --sample_time_interval=3 --migration_only=1 --n_nop=20000 --v=8 

# for protocol in protocols: 
#   for i in range(len(ratios)):
#     ratio = ratios[i]
#     cmd = get_cmd(n, i)
#     print('./bench_tpcc --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --partitioner=hash2 --query=mixed --neworder_dist=%d --payment_dist=%d' % (id, cmd, protocol, 12*n, ratio, ratio))   
      