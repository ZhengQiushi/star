import sys

ips = [line.strip() for line in open("ips.txt", "r")]

id = int(sys.argv[1])
port = int(sys.argv[2]) 


protocols = ["Star"]
ns = [2]
ps = [1, 5, 10, 15]

for n in ns:
  if id >= n:
    break
  ins = [line.split(" ")[0] for line in ips[0:n]]
  outs = [line.split(" ")[1] for line in ips[0:n]]
  print(ins, outs)
  for p in ps:
    for i in range(3):
      cmd = ""
      for j in range(n):
        if j > 0:
          cmd += ";"
        if id == j:
          cmd += ins[j] + ":" + str(port+i)
        else:
          cmd += outs[j] + ":" + str(port+i)
      print('./bench_ycsb --logtostderr=1 --id=%d --servers="%s" --protocol=Star --partition_num=%d --partitioner=hash2 --threads=12 --read_write_ratio=90 --cross_ratio=%d --batch_size=1000 --batch_flush=200' % (id, cmd, 12*n, p))
