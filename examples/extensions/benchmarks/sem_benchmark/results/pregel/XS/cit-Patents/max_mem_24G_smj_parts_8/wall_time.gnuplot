set terminal pngcairo size 1400,700 enhanced font 'Sans,11'
set output '/home/ubuntu/sail/examples/extensions/benchmarks/sem_benchmark/results/pregel/XS/cit-Patents/max_mem_24G_smj_parts_8/wall_time.png'
set title "pregel pagerank / cit-Patents (XS) — max_mem_24G_smj_parts_8\nmedian=14.729s  mean=14.800s  std=0.173s  min=14.673s  max=14.998s  p90=14.944s  p95=14.971s  runs=3"
set xlabel 'wall time (s)'
set ylabel 'probability density (1/s)'
set tmargin 5
set yrange [0:3.26162]
set xrange [14.261216:15.410092]
set grid y
set key top right
set arrow from 14.729315,0 to 14.729315,3.26162 nohead lc rgb 'red' lw 2
set label 'median 14.729s' at 14.729315,3.26162 offset char 0,1 tc rgb 'red'
set arrow from 14.944202,0 to 14.944202,3.26162 nohead lc rgb 'orange' lw 1 dt 2
set label 'p90 14.944s' at 14.944202,3.26162 offset char 0,1 tc rgb 'orange'
set arrow from 14.971063,0 to 14.971063,3.26162 nohead lc rgb 'orange' lw 1 dt 3
set label 'p95 14.971s' at 14.971063,3.26162 offset char 0,1 tc rgb 'orange'
set arrow from 14.729315,0 to 14.729315,0.100357 nohead lc rgb '#666666' lw 1
set arrow from 14.673385,0 to 14.673385,0.100357 nohead lc rgb '#666666' lw 1
set arrow from 14.997924,0 to 14.997924,0.100357 nohead lc rgb '#666666' lw 1
plot '/home/ubuntu/sail/examples/extensions/benchmarks/sem_benchmark/results/pregel/XS/cit-Patents/max_mem_24G_smj_parts_8/wall_time.dat' using 1:2 with filledcurves y=0 lc rgb '#4682b4' fs transparent solid 0.25 title 'kernel density', \
     '/home/ubuntu/sail/examples/extensions/benchmarks/sem_benchmark/results/pregel/XS/cit-Patents/max_mem_24G_smj_parts_8/wall_time.dat' using 1:2 with lines lw 2 lc rgb '#4682b4' title 'KDE (runs)'
