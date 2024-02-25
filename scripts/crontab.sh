0 0 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/star/aws_10.py 240" &
10 0 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/star/aws_20.py 240" &
20 0 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/star/aws_50.py 240" &
30 0 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/star/aws_80.py 240" &
40 0 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/star/aws_100.py 240" &

0 1 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/silo/aws_10.py 240" &
10 1 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/silo/aws_20.py 240" &
20 1 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/silo/aws_50.py 240" &
30 1 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/silo/aws_80.py 240" &
40 1 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/silo/aws_100.py 240" &

0 2 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/leap/aws_10.py 240" &
10 2 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/leap/aws_20.py 240" &
20 2 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/leap/aws_50.py 240" &
30 2 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/leap/aws_80.py 240" &
40 2 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/leap/aws_100.py 240" &

0 3 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/clay/aws_10.py 240" &
10 3 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/clay/aws_20.py 240" &
20 3 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/clay/aws_50.py 240" &
30 3 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/clay/aws_80.py 240" &
40 3 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/clay/aws_100.py 240" &

2 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/lion/aws_10.py 240" &
10 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/lion/aws_20.py 240" &
20 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/lion/aws_50.py 240" &
30 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/lion/aws_80.py 240" &
40 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh skew_ratio/lion/aws_100.py 240" &




# timeline for dm
0 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/timeline/aria/aws_0.py 360" &
10 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/timeline/aria/aws_50.py 360" &
20 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/timeline/aria/aws_80.py 360" &


30 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/timeline/calvin/aws_0.py 360" &
40 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/timeline/calvin/aws_50.py 360" &
50 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/timeline/calvin/aws_80.py 360" &


0 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/timeline/hermes/aws_0.py 360" &
10 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/timeline/hermes/aws_50.py 360" &
20 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/timeline/hermes/aws_80.py 360" &


# dist ratio for dm 
# skew
0 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/aria/aws_0.py 240" &
10 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/aria/aws_20.py 240" &
20 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/aria/aws_50.py 240" &
30 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/aria/aws_80.py 240" &
40 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/aria/aws_100.py 240" &

0 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/calvin/aws_0.py 240" &
10 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/calvin/aws_20.py 240" &
20 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/calvin/aws_50.py 240" &
30 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/calvin/aws_80.py 240" &
40 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/calvin/aws_100.py 240" &


0 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/hermes/aws_0.py 240" &
10 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/hermes/aws_20.py 240" &
20 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/hermes/aws_50.py 240" &
30 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/hermes/aws_80.py 240" &
40 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/hermes/aws_100.py 240" &

# uniform 
0 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/aria/aws_0.py 240" &
10 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/aria/aws_20.py 240" &
20 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/aria/aws_50.py 240" &
30 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/aria/aws_80.py 240" &
40 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/aria/aws_100.py 240" &


0 18 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/calvin/aws_0.py 240" &
10 18 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/calvin/aws_20.py 240" &
20 18 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/calvin/aws_50.py 240" &
30 18 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/calvin/aws_80.py 240" &
40 18 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/calvin/aws_100.py 240" &


0 19 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/hermes/aws_0.py 240" &
10 19 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/hermes/aws_20.py 240" &
20 19 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/hermes/aws_50.py 240" &
30 19 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/hermes/aws_80.py 240" &
40 19 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/hermes/aws_100.py 240" &



# dist ratio for nondm 
# skew
0 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dist_ratio/skew/aria/aws_0.py 180" &
10 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dist_ratio/skew/aria/aws_20.py 180" &
20 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dist_ratio/skew/aria/aws_50.py 180" &
30 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dist_ratio/skew/aria/aws_80.py 180" &
40 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dist_ratio/skew/aria/aws_100.py 180" &

0 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dist_ratio/skew/calvin/aws_0.py 180" &
10 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dist_ratio/skew/calvin/aws_20.py 180" &
20 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dist_ratio/skew/calvin/aws_50.py 180" &
30 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dist_ratio/skew/calvin/aws_80.py 180" &
40 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dist_ratio/skew/calvin/aws_100.py 180" &



# uniform 
0 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/aria/aws_0.py 180" &
10 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/aria/aws_20.py 180" &
20 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/aria/aws_50.py 180" &
30 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/aria/aws_80.py 180" &
40 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/aria/aws_100.py 180" &


0 18 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/calvin/aws_0.py 180" &
10 18 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/calvin/aws_20.py 180" &
20 18 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/calvin/aws_50.py 180" &
30 18 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/calvin/aws_80.py 180" &
40 18 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/calvin/aws_100.py 180" &


0 19 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/hermes/aws_0.py 180" &
10 19 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/hermes/aws_20.py 180" &
20 19 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/hermes/aws_50.py 180" &
30 19 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/hermes/aws_80.py 180" &
40 19 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/uniform/hermes/aws_100.py 180" &



40 19 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/hstore/aws_100.py 180" &

30 21 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh  1_1_s_dist_ratio/uniform/hstore/aws_0.py 180" &
35 21 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/hstore/aws_20.py 180" &
40 21 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/hstore/aws_50.py 180" &
45 21 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/hstore/aws_80.py 180" &
50 21 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/hstore/aws_100.py 180" &

55 21 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh  1_1_s_dist_ratio/skew/hstore/aws_0.py 180" &
00 22 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/hstore/aws_20.py 180" &
05 22 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/hstore/aws_50.py 180" &
10 22 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/hstore/aws_80.py 180" &
15 22 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/hstore/aws_100.py 180" &


00 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh  1_1_2_s_dist_ratio_tpcc/uniform/hstore/aws_10.py 240" &
05 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/hstore/aws_20.py  240" &
10 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/hstore/aws_50.py  240" &
15 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/hstore/aws_80.py  240" &
20 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/hstore/aws_100.py 240" &

25 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/hstore/aws_10.py 240" &
30 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/hstore/aws_20.py 240" &
35 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/hstore/aws_50.py 240" &
40 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/hstore/aws_80.py 240" &
45 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/hstore/aws_100.py 240" &

50 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh  1_1_s_dist_ratio/uniform/hstore/aws_0.py 240" &
55 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/hstore/aws_20.py 240" &
00 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/hstore/aws_50.py 240" &
05 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/hstore/aws_80.py 240" &
10 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/hstore/aws_100.py 240" &

15 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh  1_1_s_dist_ratio/skew/hstore/aws_0.py 240" &
20 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/hstore/aws_20.py 240" &
25 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/hstore/aws_50.py 240" &
30 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/hstore/aws_80.py 240" &
35 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/hstore/aws_100.py 240" &

30 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/hstore/aws_100_240.py 500" &