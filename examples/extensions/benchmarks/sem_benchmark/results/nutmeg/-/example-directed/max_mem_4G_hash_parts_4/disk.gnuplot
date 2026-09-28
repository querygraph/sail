set terminal pngcairo size 1400,700 enhanced font 'Sans,11'
set output '/var/home/sem/github/sail/examples/extensions/benchmarks/sem_benchmark/results/nutmeg/-/example-directed/max_mem_4G_hash_parts_4/disk.png'
set title "nutmeg pagerank / example-directed (-) — max_mem_4G_hash_parts_4 — disk usage"
set xlabel 'fraction of run (%)'
set ylabel 'disk consumed (GiB)'
set grid
set key top left
set yrange [0:*]
plot '/var/home/sem/github/sail/examples/extensions/benchmarks/sem_benchmark/results/nutmeg/-/example-directed/max_mem_4G_hash_parts_4/disk.dat' using 1:3:4 with filledcurves lc rgb '#cccccc' title '95% CI', \
     '' using 1:2 with lines lw 2 lc rgb '#4682b4' title 'mean'
