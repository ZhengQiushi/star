scp -i ~/.ssh/zzh_cloud -r docker-18.03.0-ce.tgz centos@10.77.70.250:/home/centos/
scp -i ~/.ssh/zzh_cloud -r docker.service centos@10.77.70.250:/home/centos/
scp -i ~/.ssh/zzh_cloud -r zqs_image.tar centos@10.77.70.250:/home/centos/


ssh centos@10.77.70.250 -i .ssh/zzh_cloud

tar zxf docker-18.03.0-ce.tgz && sudo mv docker/* /usr/bin/
sudo mv docker.service /etc/systemd/system/
sudo chmod 777 /etc/systemd/system/docker.service
sudo systemctl daemon-reload
sudo systemctl start docker
sudo systemctl enable docker
sudo chmod 777 /var/run/docker.sock

docker load < zqs_image.tar
sudo docker volume create zqs-vol
sudo chmod -R 777 /home/docker/volumes/
mkdir -p /home/docker/volumes/zqs-vol/_data/star

docker run -it --mount source=zqs-vol,target=/home  --network host --security-opt seccomp=unconfined   --name zqs_0 413366511/ubuntu  /bin/bash 
exit
docker container start zqs_0
docker exec -it zqs_0 bash


# docker save -o coredns.tar k8s.gcr.io/coredns:1.3.1

232
233
234
235

232-253

