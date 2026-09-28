set terminal pngcairo size 1400,700 enhanced font 'Sans,11'
set output '/home/ubuntu/sail/examples/extensions/benchmarks/sem_benchmark/results/pregel/M/graph500-24/max_mem_24G_smj_parts_8/wall_time.png'
set title "pregel pagerank / graph500-24 (M) — max_mem_24G_smj_parts_8\nmedian=181.193s  mean=179.442s  std=6.653s  min=172.089s  max=185.043s  p90=184.273s  p95=184.658s  runs=3"
set xlabel 'wall time (s)'
set ylabel 'probability density (1/s)'
set tmargin 5
set yrange [0:0.0765813]
set xrange [155.636367:201.495442]
set grid y
set key top right
set arrow from 181.193367,0 to 181.193367,0.0765813 nohead lc rgb 'red' lw 2
set label 'median 181.193s' at 181.193367,0.0765813 offset char 0,1 tc rgb 'red'
set arrow from 184.273187,0 to 184.273187,0.0765813 nohead lc rgb 'orange' lw 1 dt 2
set label 'p90 184.273s' at 184.273187,0.0765813 offset char 0,1 tc rgb 'orange'
set arrow from 184.658164,0 to 184.658164,0.0765813 nohead lc rgb 'orange' lw 1 dt 3
set label 'p95 184.658s' at 184.658164,0.0765813 offset char 0,1 tc rgb 'orange'
set arrow from 172.088667,0 to 172.088667,0.00235635 nohead lc rgb '#666666' lw 1
set arrow from 181.193367,0 to 181.193367,0.00235635 nohead lc rgb '#666666' lw 1
set arrow from 185.043142,0 to 185.043142,0.00235635 nohead lc rgb '#666666' lw 1
plot '/home/ubuntu/sail/examples/extensions/benchmarks/sem_benchmark/results/pregel/M/graph500-24/max_mem_24G_smj_parts_8/wall_time.dat' using 1:2 with filledcurves y=0 lc rgb '#4682b4' fs transparent solid 0.25 title 'kernel density', \
     '/home/ubuntu/sail/examples/extensions/benchmarks/sem_benchmark/results/pregel/M/graph500-24/max_mem_24G_smj_parts_8/wall_time.dat' using 1:2 with lines lw 2 lc rgb '#4682b4' title 'KDE (runs)'
