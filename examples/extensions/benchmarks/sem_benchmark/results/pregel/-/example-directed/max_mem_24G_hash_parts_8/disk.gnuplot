set terminal pngcairo size 1400,700 enhanced font 'Sans,11'
set output '/home/ubuntu/sail/examples/extensions/benchmarks/sem_benchmark/results/pregel/-/example-directed/max_mem_24G_hash_parts_8/disk.png'
set title "pregel pagerank / example-directed (-) — max_mem_24G_hash_parts_8 — disk usage"
set xlabel 'fraction of run (%)'
set ylabel 'disk consumed (GiB)'
set grid
set key top left
set yrange [0:*]
plot '/home/ubuntu/sail/examples/extensions/benchmarks/sem_benchmark/results/pregel/-/example-directed/max_mem_24G_hash_parts_8/disk.dat' using 1:3:4 with filledcurves lc rgb '#cccccc' title '95% CI', \
     '' using 1:2 with lines lw 2 lc rgb '#4682b4' title 'mean'
