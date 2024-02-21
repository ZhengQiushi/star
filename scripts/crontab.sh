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
0 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/aria/aws_0.py 180" &
10 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/aria/aws_20.py 180" &
20 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/aria/aws_50.py 180" &
30 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/aria/aws_80.py 180" &
40 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh dm/dist_ratio/skew/aria/aws_100.py 180" &

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

###

00 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/siloS/aws_10.py 180" &
10 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/siloS/aws_20.py 180" &
20 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/siloS/aws_50.py 180" &
30 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/siloS/aws_80.py 180" &
40 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/siloS/aws_100.py 180" &

00 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/siloS/aws_10.py 180" &
10 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/siloS/aws_20.py 180" &
20 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/siloS/aws_50.py 180" &
30 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/siloS/aws_80.py 180" &
40 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/siloS/aws_100.py 180" &


00 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_10.py 200" &
10 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_20.py 200" &
20 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_50.py 200" &
30 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_80.py 200" &
40 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_100.py 200" &

00 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/leapS/aws_10.py 200" &
10 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/leapS/aws_20.py 200" &
20 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/leapS/aws_50.py 200" &
30 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/leapS/aws_80.py 200" &
40 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/leapS/aws_100.py 200" &



00 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/lionS/aws_10.py 200" &
10 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/lionS/aws_20.py 200" &
20 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/lionS/aws_50.py 200" &
30 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/lionS/aws_80.py 200" &
40 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/lionS/aws_100.py 200" &

00 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/lionS/aws_10.py 200" &
10 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/lionS/aws_20.py 200" &
20 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/lionS/aws_50.py 200" &
30 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/lionS/aws_80.py 200" &
40 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/lionS/aws_100.py 200" &



# skew
10 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/siloS/aws_10.py 180" &
15 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/siloS/aws_20.py 180" &
20 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/siloS/aws_50.py 180" &
25 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/siloS/aws_80.py 180" &
30 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/siloS/aws_100.py 180" &





00 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/lionS/aws_10.py  200" &
05 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/lionS/aws_20.py  200" &
10 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/lionS/aws_50.py  200" &
15 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/lionS/aws_80.py  200" &
20 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/lionS/aws_100.py 200" &





20 20 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/leapS/aws_20.py  200" &
28 20 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/leapS/aws_50.py  200" &
36 20 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/leapS/aws_80.py  200" &
42 20 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/leapS/aws_100.py 200" &
50 20 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/leapS/aws_10.py  200" &

58 20 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/clayS/aws_20.py  200" &
06 21 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/clayS/aws_50.py  200" &
14 20 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/clayS/aws_80.py  200" &
22 20 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/clayS/aws_100.py 200" &
30 20 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/clayS/aws_10.py  200" &

25 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/clayS/aws_20.py  200" &
30 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/clayS/aws_50.py  200" &
35 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/clayS/aws_80.py  200" &
40 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/clayS/aws_100.py 200" &
45 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_s_skew_ratio/clayS/aws_10.py  200" &




00 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/clayS/aws_10.py 200" &
05 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/clayS/aws_20.py 200" &
10 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/clayS/aws_50.py 200" &
15 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/clayS/aws_80.py 200" &
20 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/clayS/aws_100.py 200" &


25 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/clayS/aws_10.py 200" &
30 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/clayS/aws_20.py 200" &
35 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/clayS/aws_50.py 200" &
40 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/clayS/aws_80.py 200" &
45 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/clayS/aws_100.py 200" &

50 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/lionS/aws_10.py 200" &
55 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/lionS/aws_20.py 200" &
00 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/lionS/aws_50.py 200" &
05 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/lionS/aws_80.py 200" &
10 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/lionS/aws_100.py 200" &

15 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/lionS/aws_10.py 200" &
20 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/lionS/aws_20.py 200" &
25 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/lionS/aws_50.py 200" &
30 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/lionS/aws_80.py 200" &
35 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/lionS/aws_100.py 200" &

40 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/leapS/aws_10.py 200" &
45 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/leapS/aws_20.py 200" &
50 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/leapS/aws_50.py 200" &
55 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/leapS/aws_80.py 200" &
00 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/uniform/leapS/aws_100.py 200" &

05 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_10.py 200" &
10 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_20.py 200" &
15 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_50.py 200" &
20 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_80.py 200" &
25 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_100.py 200" &


#TIMELINE
50 05 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_2_timeline/skew/lionS/aws_100.py 500" &
00 06 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_2_timeline/skew/leapS/aws_100.py 500" &
10 06 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_2_timeline/skew/clayS/aws_100.py 500" &



00 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/siloS/aws_10.py 180" &
10 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/siloS/aws_20.py 180" &
20 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/siloS/aws_50.py 180" &
30 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/siloS/aws_80.py 180" &
40 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/siloS/aws_100.py 180" &


00 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_10.py 200" &
10 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_20.py 200" &
20 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_50.py 200" &
30 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_80.py 200" &
40 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_s_dist_ratio/skew/leapS/aws_100.py 200" &


# tpcc



40 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/siloS/aws_20.py  240" &
45 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/siloS/aws_50.py  240" &
50 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/siloS/aws_80.py  240" &
55 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/siloS/aws_10.py  240" &
00 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/siloS/aws_100.py 240" &


# 0218
20 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/lionS/aws_10.py  200" &
25 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/lionS/aws_20.py  200" &
30 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/lionS/aws_50.py  200" &
35 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/lionS/aws_80.py  200" &
40 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/lionS/aws_100.py 200" &


45 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/leapS/aws_10.py  200" &
50 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/leapS/aws_20.py  200" &
55 13 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/leapS/aws_50.py  200" &
00 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/leapS/aws_80.py  200" &
05 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/leapS/aws_100.py 200" &

10 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/clayS/aws_10.py  200" &
15 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/clayS/aws_20.py  200" &
20 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/clayS/aws_50.py  200" &
25 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/clayS/aws_80.py  200" &
30 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/clayS/aws_100.py 200" &


# skew
# 0218
35 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/lionS/aws_10.py  240" &
40 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/lionS/aws_20.py  240" &
45 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/lionS/aws_50.py  240" &
50 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/lionS/aws_80.py  240" &
55 14 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/lionS/aws_100.py 240" &


00 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/leapS/aws_10.py  240" &
05 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/leapS/aws_20.py  240" &
10 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/leapS/aws_50.py  240" &
15 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/leapS/aws_80.py  240" &
20 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/leapS/aws_100.py 240" &



25 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/clayS/aws_10.py  200" &
30 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/clayS/aws_20.py  200" &
35 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/clayS/aws_50.py  200" &
40 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/clayS/aws_80.py  200" &
45 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/uniform/clayS/aws_100.py 200" &

# tpcc skew
40 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/siloS/aws_10.py 180" &
45 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/siloS/aws_20.py 180" &
50 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/siloS/aws_50.py 180" &
55 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/siloS/aws_80.py 180" &
00 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/siloS/aws_100.py 180" &


05 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/leapS/aws_10.py 220" &
10 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/leapS/aws_20.py 220" &
15 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/leapS/aws_50.py 220" &
20 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/leapS/aws_80.py 220" &
25 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/leapS/aws_100.py 220" &


30 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/lionS/aws_10.py  220" &
35 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/lionS/aws_20.py  220" &
40 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/lionS/aws_50.py  220" &
45 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/lionS/aws_80.py  220" &
50 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/lionS/aws_100.py 220" &
# tpcc skew done




# skew
00 01 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/siloS/aws_10.py  200" &
10 01 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/siloS/aws_20.py  200" &
20 01 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/siloS/aws_50.py  200" &
30 01 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/siloS/aws_80.py  200" &
40 01 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/siloS/aws_100.py 200" &


00 02 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/lionS/aws_10.py  200" &
10 02 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/lionS/aws_20.py  200" &
20 02 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/lionS/aws_50.py  200" &
30 02 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/lionS/aws_80.py  200" &
40 02 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/lionS/aws_100.py 200" &



00 03 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/leapS/aws_20.py  200" &
10 03 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/leapS/aws_50.py  200" &
20 03 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/leapS/aws_80.py  200" &
30 03 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/leapS/aws_100.py 200" &
40 03 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_3_2_s_skew_ratio_tpcc/leapS/aws_10.py  200" &

# 
20 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/clayS/aws_10.py  200" &
30 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/clayS/aws_20.py  200" &
40 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/clayS/aws_50.py  200" &
50 10 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/clayS/aws_80.py  200" &
00 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 1_1_2_s_dist_ratio_tpcc/skew/clayS/aws_100.py  200" &



00 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips5.txt  scale/ycsb/silo_10.py  500" &
10 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips6.txt  scale/ycsb/silo_10.py  500" &
20 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips7.txt  scale/ycsb/silo_10.py  500" &
30 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips8.txt  scale/ycsb/silo_10.py  500" &
40 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips9.txt  scale/ycsb/silo_10.py  500" &
50 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips10.txt  scale/ycsb/silo_10.py 500" &

00 01 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips11.txt  scale/ycsb/silo_10.py 500" &





40 22 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips8.txt   scale/ycsb/leap_10.py 350" &
50 22 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips9.txt   scale/ycsb/leap_10.py 350" &
00 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips10.txt  scale/ycsb/leap_10.py 350" &
10 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips11.txt  scale/ycsb/leap_10.py 350" &
20 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips5.txt   scale/ycsb/leap_10.py 350" &
30 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips6.txt   scale/ycsb/leap_10.py 200" &
40 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips7.txt   scale/ycsb/leap_10.py 350" &

40 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips4.txt   scale/ycsb/lionb_10.py 400" &



15 20 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips10.txt  scale/ycsb/lionbb_10.py 200" &
25 20 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips5.txt   scale/ycsb/lionbb_10.py 200" &
35 20 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips6.txt   scale/ycsb/lionbb_10.py 200" &
45 20 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips7.txt   scale/ycsb/lionbb_10.py 200" &
55 20 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips9.txt   scale/ycsb/lionbb_10.py 200" &
05 21 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips8.txt   scale/ycsb/lionbb_10.py 200" &
15 21 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips11.txt  scale/ycsb/lionbb_10.py 200" &




20 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips9.txt   scale/ycsb/lionblog_10.py 500" &
30 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips10.txt  scale/ycsb/lionblog_10.py 500" &
40 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips11.txt  scale/ycsb/lionblog_10.py 500" &
50 11 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips5.txt   scale/ycsb/lionblog_10.py 500" &
00 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips6.txt   scale/ycsb/lionblog_10.py 500" &
10 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips7.txt   scale/ycsb/lionblog_10.py 500" &
20 12 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips8.txt   scale/ycsb/lionblog_10.py 500" &

25 21 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips4.txt   scale/ycsb/lionblog_10.py 200" &
35 21 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips5.txt   scale/ycsb/lionblog_10.py 200" &
45 21 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips6.txt   scale/ycsb/lionblog_10.py 200" &




55 21 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips5.txt   scale/ycsb/calvin_10.py 200" &
05 22 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips6.txt   scale/ycsb/calvin_10.py 200" &
15 22 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips7.txt   scale/ycsb/calvin_10.py 200" &
25 22 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips8.txt   scale/ycsb/calvin_10.py 200" &
35 22 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips9.txt   scale/ycsb/calvin_10.py 200" &
45 22 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips10.txt  scale/ycsb/calvin_10.py 200" &
55 22 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips11.txt  scale/ycsb/calvin_10.py 200" &



05 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips6.txt   scale/ycsb/hermes_10.py 200" &
15 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips7.txt   scale/ycsb/hermes_10.py 200" &
25 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips8.txt   scale/ycsb/hermes_10.py 200" &
35 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips9.txt   scale/ycsb/hermes_10.py 200" &
45 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips10.txt  scale/ycsb/hermes_10.py 200" &
55 23 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips11.txt  scale/ycsb/hermes_10.py 200" &
05 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips5.txt   scale/ycsb/hermes_10.py 200" &


15 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips6.txt   scale/ycsb/aria_10.py 200" &
25 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips7.txt   scale/ycsb/aria_10.py 200" &
35 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips8.txt   scale/ycsb/aria_10.py 200" &
45 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips9.txt   scale/ycsb/aria_10.py 200" &
55 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips10.txt  scale/ycsb/aria_10.py 200" &
05 01 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips11.txt  scale/ycsb/aria_10.py 200" &
15 01 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips5.txt   scale/ycsb/aria_10.py 200" &

25 01 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips9.txt   scale/ycsb/lionbb_10.py 200" &

35 01 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips6.txt   scale/ycsb/star_10.py 200" &
45 01 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips7.txt   scale/ycsb/star_10.py 200" &
55 01 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips8.txt   scale/ycsb/star_10.py 200" &
05 02 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips9.txt   scale/ycsb/star_10.py 200" &
15 02 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips10.txt  scale/ycsb/star_10.py 200" &
25 02 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips11.txt  scale/ycsb/star_10.py 200" &
35 02 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips5.txt   scale/ycsb/star_10.py 200" &



45 02 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips6.txt   scale/ycsb/hermes_10.py 200" &
55 02 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips7.txt   scale/ycsb/hermes_10.py 200" &
05 03 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips8.txt   scale/ycsb/hermes_10.py 200" &
15 03 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips9.txt   scale/ycsb/hermes_10.py 200" &
25 03 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips10.txt  scale/ycsb/hermes_10.py 200" &
35 03 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips11.txt  scale/ycsb/hermes_10.py 200" &
45 03 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips5.txt   scale/ycsb/hermes_10.py 200" &




55 03 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips5.txt   scale/ycsb/calvin_10.py 200" &
05 04 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips6.txt   scale/ycsb/calvin_10.py 200" &
15 04 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips7.txt   scale/ycsb/calvin_10.py 200" &
25 04 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips8.txt   scale/ycsb/calvin_10.py 200" &
35 04 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips9.txt   scale/ycsb/calvin_10.py 200" &
45 04 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips10.txt  scale/ycsb/calvin_10.py 200" &
55 04 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips11.txt  scale/ycsb/calvin_10.py 200" &


05 05 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips5.txt   scale/ycsb/clays_10.py 200" &
15 05 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips6.txt   scale/ycsb/clays_10.py 200" &
25 05 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips7.txt   scale/ycsb/clays_10.py 200" &
35 05 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips8.txt   scale/ycsb/clays_10.py 200" &
45 05 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips9.txt   scale/ycsb/clays_10.py 200" &
55 05 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips10.txt  scale/ycsb/clays_10.py 200" &
05 06 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips11.txt  scale/ycsb/clays_10.py 200" &



10 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips8.txt   scale/ycsb/lion_10.py 500" &
20 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips9.txt   scale/ycsb/lion_10.py 500" &
30 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips11.txt  scale/ycsb/lion_10.py 500" &
40 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips10.txt  scale/ycsb/lion_10.py 500" &
50 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips5.txt   scale/ycsb/lion_10.py 500" &
00 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips6.txt   scale/ycsb/lion_10.py 500" &
10 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips7.txt   scale/ycsb/lion_10.py 500" &


20 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips8.txt   scale/ycsb/leap_10.py 500" &
30 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips9.txt   scale/ycsb/leap_10.py 500" &
40 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips10.txt  scale/ycsb/leap_10.py 500" &
50 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips11.txt  scale/ycsb/leap_10.py 500" &
00 18 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips5.txt   scale/ycsb/leap_10.py 200" &
10 18 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips6.txt   scale/ycsb/leap_10.py 500" &
20 18 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/scale/test.sh /home/star/scripts/scale/ips7.txt   scale/ycsb/leap_10.py 500" &


# 0220
00 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/leapS/aws_10.py 200" &
05 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/leapS/aws_20.py 200" &
10 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/leapS/aws_50.py 200" &
15 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/leapS/aws_80.py 200" &
20 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/leapS/aws_100.py 200" &

25 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionR/aws_10.py 200" &
30 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionR/aws_20.py 200" &
35 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionR/aws_50.py 200" &
40 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionR/aws_80.py 200" &
45 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionR/aws_100.py 200" &

# 
50 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRB/aws_10.py 200" &
55 15 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRB/aws_20.py 200" &
00 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRB/aws_50.py 200" &
05 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRB/aws_80.py 200" &
10 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRB/aws_100.py 200" &
# 
15 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRW/aws_10.py 200" &
20 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRW/aws_20.py 200" &
25 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRW/aws_50.py 200" &
30 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRW/aws_80.py 200" &
35 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRW/aws_100.py 200" &

40 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRWB/aws_10.py 200" &
45 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRWB/aws_20.py 200" &
50 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRWB/aws_50.py 200" &
55 16 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRWB/aws_80.py 200" &
00 17 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionRWB/aws_100.py 200" &



40 08 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionS/aws_10.py 200" &
35 08 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionS/aws_20.py 200" &
50 08 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionS/aws_50.py 200" &
45 08 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionS/aws_80.py 200" &
55 08 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionS/aws_100.py 200" &

00 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionSW/aws_10.py 200" &
05 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionSW/aws_20.py 200" &
10 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionSW/aws_50.py 200" &
15 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionSW/aws_80.py 200" &
20 09 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lionSW/aws_100.py 200" &


20 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lion/aws_10.py 200" &
15 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lion/aws_20.py 200" &
10 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lion/aws_50.py 200" &
05 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lion/aws_80.py 200" &
00 00 * * * docker exec zqs_4 bash -c "bash /home/star/scripts/test.sh 2_module/uniform/lion/aws_100.py 200" &