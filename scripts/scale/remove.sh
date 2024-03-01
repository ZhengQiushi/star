ps aux | grep bench_ycsb | awk '{print $2}' | xargs kill -9
rm -rf /home/star/data/commit/*