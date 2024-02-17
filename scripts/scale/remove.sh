ps aux | grep zqs_laji | awk '{print $2}' | xargs kill -9
rm -rf /home/star/data/commit/*