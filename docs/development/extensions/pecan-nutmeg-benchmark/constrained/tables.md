# All PageRank and WCC methods

Cells show median [minimum, maximum] for passed trials only; n is the available sample count for that metric. Counts retain every outcome.

RSS/PSS are sampled execution-phase process totals. Cgroup peak is the lifetime peak through result delivery, before verification. Memory is MiB.


## chain-diagnostic / chain-512 / process-cluster / wcc

| Path | Variant | Outcomes | Seconds | PSS MiB | RSS MiB | Cgroup MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Grenada | optimized | not_run: 1 | unavailable | unavailable | unavailable | unavailable |
| Grenada | reference | not_run: 1 | unavailable | unavailable | unavailable | unavailable |
| Banda | optimized | not_run: 1 | unavailable | unavailable | unavailable | unavailable |
| Banda | reference | not_run: 1 | unavailable | unavailable | unavailable | unavailable |
| Pecan | optimized | not_run: 1 | unavailable | unavailable | unavailable | unavailable |
| Pecan | reference | not_run: 1 | unavailable | unavailable | unavailable | unavailable |

## distributed / sparse-10000 / process-cluster / pagerank

| Path | Variant | Outcomes | Seconds | PSS MiB | RSS MiB | Cgroup MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Grenada | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Grenada | reference | not_run: 2, passed: 1 | 8.284 [8.284, 8.284] (n=1) | 1032.943 [1032.943, 1032.943] (n=1) | 1170.684 [1170.684, 1170.684] (n=1) | 1016.660 [1016.660, 1016.660] (n=1) |
| Banda | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Banda | reference | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Pecan | optimized | not_run: 2, passed: 1 | 11.072 [11.072, 11.072] (n=1) | 1145.733 [1145.733, 1145.733] (n=1) | 1285.133 [1285.133, 1285.133] (n=1) | 1152.621 [1152.621, 1152.621] (n=1) |
| Pecan | reference | not_run: 2, passed: 1 | 8.265 [8.265, 8.265] (n=1) | 1070.654 [1070.654, 1070.654] (n=1) | 1207.426 [1207.426, 1207.426] (n=1) | 1041.621 [1041.621, 1041.621] (n=1) |

## distributed / sparse-10000 / process-cluster / wcc

| Path | Variant | Outcomes | Seconds | PSS MiB | RSS MiB | Cgroup MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Grenada | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Grenada | reference | not_run: 2, passed: 1 | 0.914 [0.914, 0.914] (n=1) | 957.249 [957.249, 957.249] (n=1) | 1092.578 [1092.578, 1092.578] (n=1) | 831.430 [831.430, 831.430] (n=1) |
| Banda | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Banda | reference | not_run: 2, passed: 1 | 0.117 [0.117, 0.117] (n=1) | 555.343 [555.343, 555.343] (n=1) | 677.180 [677.180, 677.180] (n=1) | 416.316 [416.316, 416.316] (n=1) |
| Pecan | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Pecan | reference | not_run: 3 | unavailable | unavailable | unavailable | unavailable |

## distributed / sparse-100000 / process-cluster / pagerank

| Path | Variant | Outcomes | Seconds | PSS MiB | RSS MiB | Cgroup MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Grenada | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Grenada | reference | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Banda | optimized | not_run: 2, passed: 1 | 1.325 [1.325, 1.325] (n=1) | 763.811 [763.811, 763.811] (n=1) | 888.445 [888.445, 888.445] (n=1) | 702.293 [702.293, 702.293] (n=1) |
| Banda | reference | not_run: 2, passed: 1 | 0.877 [0.877, 0.877] (n=1) | 726.924 [726.924, 726.924] (n=1) | 852.086 [852.086, 852.086] (n=1) | 672.879 [672.879, 672.879] (n=1) |
| Pecan | optimized | not_run: 2, passed: 1 | 17.024 [17.024, 17.024] (n=1) | 2509.807 [2509.807, 2509.807] (n=1) | 2650.027 [2650.027, 2650.027] (n=1) | 2656.750 [2656.750, 2656.750] (n=1) |
| Pecan | reference | not_run: 3 | unavailable | unavailable | unavailable | unavailable |

## distributed / sparse-100000 / process-cluster / wcc

| Path | Variant | Outcomes | Seconds | PSS MiB | RSS MiB | Cgroup MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Grenada | optimized | error: 1, not_run: 2 | unavailable | unavailable | unavailable | unavailable |
| Grenada | reference | not_run: 2, passed: 1 | 1.341 [1.341, 1.341] (n=1) | 1340.109 [1340.109, 1340.109] (n=1) | 1477.676 [1477.676, 1477.676] (n=1) | 1261.395 [1261.395, 1261.395] (n=1) |
| Banda | optimized | not_run: 2, passed: 1 | 0.893 [0.893, 0.893] (n=1) | 751.581 [751.581, 751.581] (n=1) | 876.352 [876.352, 876.352] (n=1) | 642.816 [642.816, 642.816] (n=1) |
| Banda | reference | not_run: 2, passed: 1 | 0.683 [0.683, 0.683] (n=1) | 706.764 [706.764, 706.764] (n=1) | 832.055 [832.055, 832.055] (n=1) | 619.141 [619.141, 619.141] (n=1) |
| Pecan | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Pecan | reference | not_run: 3 | unavailable | unavailable | unavailable | unavailable |

## distributed / sparse-1000000 / process-cluster / pagerank

| Path | Variant | Outcomes | Seconds | PSS MiB | RSS MiB | Cgroup MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Grenada | optimized | not_run: 2, passed: 1 | 65.452 [65.452, 65.452] (n=1) | 2176.697 [2176.697, 2176.697] (n=1) | 2315.711 [2315.711, 2315.711] (n=1) | 2442.527 [2442.527, 2442.527] (n=1) |
| Grenada | reference | not_run: 2, passed: 1 | 50.247 [50.247, 50.247] (n=1) | 1858.147 [1858.147, 1858.147] (n=1) | 1993.848 [1993.848, 1993.848] (n=1) | 2062.492 [2062.492, 2062.492] (n=1) |
| Banda | optimized | error: 1, not_run: 2 | unavailable | unavailable | unavailable | unavailable |
| Banda | reference | error: 1, not_run: 2 | unavailable | unavailable | unavailable | unavailable |
| Pecan | optimized | not_run: 2, passed: 1 | 65.831 [65.831, 65.831] (n=1) | 2172.057 [2172.057, 2172.057] (n=1) | 2302.844 [2302.844, 2302.844] (n=1) | 2404.180 [2404.180, 2404.180] (n=1) |
| Pecan | reference | not_run: 2, passed: 1 | 57.771 [57.771, 57.771] (n=1) | 1828.471 [1828.471, 1828.471] (n=1) | 1958.035 [1958.035, 1958.035] (n=1) | 2034.746 [2034.746, 2034.746] (n=1) |

## distributed / sparse-1000000 / process-cluster / wcc

| Path | Variant | Outcomes | Seconds | PSS MiB | RSS MiB | Cgroup MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Grenada | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Grenada | reference | not_run: 2, passed: 1 | 8.577 [8.577, 8.577] (n=1) | 2256.354 [2256.354, 2256.354] (n=1) | 2389.461 [2389.461, 2389.461] (n=1) | 2436.719 [2436.719, 2436.719] (n=1) |
| Banda | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Banda | reference | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Pecan | optimized | error: 1, not_run: 2 | unavailable | unavailable | unavailable | unavailable |
| Pecan | reference | not_run: 3 | unavailable | unavailable | unavailable | unavailable |

## local / sparse-100000 / local / pagerank

| Path | Variant | Outcomes | Seconds | PSS MiB | RSS MiB | Cgroup MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Grenada | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Grenada | reference | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Banda | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Banda | reference | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Pecan | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Pecan | reference | not_run: 3 | unavailable | unavailable | unavailable | unavailable |

## local / sparse-100000 / local / wcc

| Path | Variant | Outcomes | Seconds | PSS MiB | RSS MiB | Cgroup MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Grenada | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Grenada | reference | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Banda | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Banda | reference | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Pecan | optimized | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
| Pecan | reference | not_run: 3 | unavailable | unavailable | unavailable | unavailable |
