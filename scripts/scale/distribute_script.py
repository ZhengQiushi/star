import sys
import os

ips = [line.strip() for line in open(str(sys.argv[3]), "r")]
n = len(ips)

ins = [line.split(" ")[0] for line in ips]
outs = [line.split(" ")[1] for line in ips]

port = int(sys.argv[1]) 
script = sys.argv[2]
ips_txt = str(sys.argv[3])

# dispatch test script
for i in range(n):
  print(" ==== python3 %s %d %d %s > run.sh %s" % (script, i, port, ips_txt, outs[i]))
  os.system("python3 %s %d %d %s > run.sh" % (script, i, port, ips_txt))
  os.system("chmod 777 run.sh")
  if i < n - 1:
    os.system("scp -i  ~/.ssh/zzh_cloud run.sh centos@%s:/home/docker/volumes/zqs-vol/_data/star/run.sh" % outs[i])
  else:
    os.system("cp run.sh /home/star/run.sh")

# # run script
# for i in range(n):
#   os.system("ssh centos@%s 'source ~/.profile; cd /data/centos; screen -dm bash run.sh'" % outs[i])