set terminal pngcairo size 1400,700 enhanced font 'Sans,11'
set output '/var/home/sem/github/sail/examples/extensions/benchmarks/sem_benchmark/results/nutmeg/-/example-directed/max_mem_4G_hash_parts_4/wall_time.png'
set title "nutmeg pagerank / example-directed (-) — max_mem_4G_hash_parts_4\nmedian=0.027s  mean=0.027s  std=0.000s  min=0.027s  max=0.027s  p90=0.027s  p95=0.027s  runs=1"
set xlabel 'wall time (s)'
set ylabel 'probability density (1/s)'
set tmargin 5
set yrange [0:0.518594]
set xrange [-3.972892:4.027108]
set grid y
set key top right
set arrow from 0.027108,0 to 0.027108,0.518594 nohead lc rgb 'red' lw 2
set label 'median 0.027s' at 0.027108,0.518594 offset char 0,1 tc rgb 'red'
set arrow from 0.027108,0 to 0.027108,0.518594 nohead lc rgb 'orange' lw 1 dt 2
set label 'p90 0.027s' at 0.027108,0.518594 offset char 0,1 tc rgb 'orange'
set arrow from 0.027108,0 to 0.027108,0.518594 nohead lc rgb 'orange' lw 1 dt 3
set label 'p95 0.027s' at 0.027108,0.518594 offset char 0,1 tc rgb 'orange'
set arrow from 0.027108,0 to 0.027108,0.0159567 nohead lc rgb '#666666' lw 1
plot '/var/home/sem/github/sail/examples/extensions/benchmarks/sem_benchmark/results/nutmeg/-/example-directed/max_mem_4G_hash_parts_4/wall_time.dat' using 1:2 with filledcurves y=0 lc rgb '#4682b4' fs transparent solid 0.25 title 'kernel density', \
     '/var/home/sem/github/sail/examples/extensions/benchmarks/sem_benchmark/results/nutmeg/-/example-directed/max_mem_4G_hash_parts_4/wall_time.dat' using 1:2 with lines lw 2 lc rgb '#4682b4' title 'KDE (runs)'
