import sys

ips = [line.strip() for line in open("/home/star/scripts/ips.txt", "r")]
n = len(ips)

# print(ips)

ins = [line.split(" ")[0] for line in ips]
outs = [line.split(" ")[1] for line in ips]

id = int(sys.argv[1]) 
port = int(sys.argv[2]) 

protocols = ["HStore"]
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
    print('/home/star/zqs_laji_tpcc --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=24 --threads=4 --partitioner=hpb --granule_count=1000 --hstore_command_logging=true --wal_group_commit_time=1000 --wal_group_commit_size=0 --batch_size=10000 --batch_flush=500 --lion_with_metis_init=0 --time_to_run=60 --workload_time=60 --sample_time_interval=3 --replica_group=2 --lock_manager=1  --neworder_dist=100 --lotus_async_repl=true --log_path=/tmp --skew_factor=0' % (id, cmd, protocol))
      
#  /home/star/zqs_laji --logtostderr=1 --id=0 --servers="10.77.70.246:10210;10.77.70.247:10210;10.77.70.248:10210;10.77.70.117:10210;10.77.110.145:10210" --threads=4 --partition_num=12    --partitioner=  --protocol=HStore --batch_size=10000 --batch_flush=500 --cross_ratio=100   --time_to_run=60 --sample_time_interval=3 --skew_factor=80
      