set terminal pngcairo size 1400,700 enhanced font 'Sans,11'
set output '/home/ubuntu/sail/examples/extensions/benchmarks/sem_benchmark/results/nutmeg/-/example-directed/max_mem_24G_hash_parts_8/wall_time.png'
set title "nutmeg pagerank / example-directed (-) — max_mem_24G_hash_parts_8\nmedian=0.017s  mean=0.017s  std=0.000s  min=0.017s  max=0.017s  p90=0.017s  p95=0.017s  runs=1"
set xlabel 'wall time (s)'
set ylabel 'probability density (1/s)'
set tmargin 5
set yrange [0:0.518594]
set xrange [-3.982855:4.017145]
set grid y
set key top right
set arrow from 0.017145,0 to 0.017145,0.518594 nohead lc rgb 'red' lw 2
set label 'median 0.017s' at 0.017145,0.518594 offset char 0,1 tc rgb 'red'
set arrow from 0.017145,0 to 0.017145,0.518594 nohead lc rgb 'orange' lw 1 dt 2
set label 'p90 0.017s' at 0.017145,0.518594 offset char 0,1 tc rgb 'orange'
set arrow from 0.017145,0 to 0.017145,0.518594 nohead lc rgb 'orange' lw 1 dt 3
set label 'p95 0.017s' at 0.017145,0.518594 offset char 0,1 tc rgb 'orange'
set arrow from 0.017145,0 to 0.017145,0.0159567 nohead lc rgb '#666666' lw 1
plot '/home/ubuntu/sail/examples/extensions/benchmarks/sem_benchmark/results/nutmeg/-/example-directed/max_mem_24G_hash_parts_8/wall_time.dat' using 1:2 with filledcurves y=0 lc rgb '#4682b4' fs transparent solid 0.25 title 'kernel density', \
     '/home/ubuntu/sail/examples/extensions/benchmarks/sem_benchmark/results/nutmeg/-/example-directed/max_mem_24G_hash_parts_8/wall_time.dat' using 1:2 with lines lw 2 lc rgb '#4682b4' title 'KDE (runs)'
