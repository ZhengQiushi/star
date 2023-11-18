scp -i ~/.ssh/zqs_key -r docker-18.03.0-ce.tgz zqs@10.77.70.111:/home/zqs/
scp -i ~/.ssh/zqs_key -r docker.service zqs@10.77.70.111:/home/zqs/
scp -i ~/.ssh/zqs_key -r zqs_image.tar zqs@10.77.70.111:/home/zqs/


scp docker.service zqs@10.77.110.144:/home/zqs/
ssh zqs@10.77.70.111 -i ~/.ssh/zqs_key

sudo sh -c "echo '$USER ALL = (ALL) NOPASSWD:ALL' > /etc/sudoers.d/$USER"


scp -i ~/.ssh/zzh_cloud -r docker-18.03.0-ce.tgz centos@10.77.70.207:/home/centos/
scp -i ~/.ssh/zzh_cloud -r docker.service centos@10.77.70.207:/home/centos/
scp -i ~/.ssh/zzh_cloud -r zqs_image.tar centos@10.77.70.207:/home/centos/


scp docker.service centos@10.77.70.111:/home/centos/
ssh centos@10.77.70.207 -i ~/.ssh/zzh_cloud

sudo sh -c "echo '$USER ALL = (ALL) NOPASSWD:ALL' > /etc/sudoers.d/$USER"
tar zxf docker-18.03.0-ce.tgz && sudo mv docker/* /usr/bin/
sudo mv docker.service /etc/systemd/system/
sudo chmod 644 /etc/systemd/system/docker.service
sudo systemctl daemon-reload
sudo systemctl start docker
sudo systemctl enable docker
sudo chmod 777 /var/run/docker.sock

docker load < zqs_image.tar
sudo docker volume create zqs-vol
sudo chmod -R 777 /home/docker/volumes/
# sudo chmod -R 777 /var/lib/docker/volumes/ 
mkdir -p /home/docker/volumes/zqs-vol/_data/star
mkdir -p /home/docker/volumes/zqs-vol/_data/star/data
sudo chmod -R 777 /home/docker/volumes/

docker run -it --mount source=zqs-vol,target=/home  --network host --security-opt seccomp=unconfined   --name zqs_0 413366511/ubuntu  /bin/bash 
# docker run -it  --network host --security-opt seccomp=unconfined   --name zqs_0 413366511/ubuntu  /bin/bash 

exit
docker container start zqs_0
docker exec -it zqs_0 bash


# docker save -o coredns.tar k8s.gcr.io/coredns:1.3.1
dockerd --log-level error
sudo journalctl -xe | grep docker

232
233
234
235

232-253
scp -r docker.service  zqs@10.77.110.144:/home/zqs/
scp -r zqs_image.tar zqs@10.77.110.144:/home/zqs/


643
622
592
589
642
633
655
621
665
681
662
661
550
564
591
629