"""Render immutable benchmark summaries; no measurements are generated here.

python render_report.py --primary primary/summary.json \
  --fusion fusion/summary.json --output figures
Requires matplotlib and numpy. Input summaries come from benchmarks/summarize.py.
"""
import argparse
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

STYLES = {
    'pecan': ('Pecan', '#2465a8'),
    'nutmeg-native': ('Banda', '#bf5b17'),
    'nutmeg-datafusion': ('Grenada', '#28834e'),
}
LABELS = {'reference': 'reference', 'optimized': 'advanced', 'fused': 'advanced fused'}


def metric(group, key, divisor=1):
    value = group['metrics'][key]
    if value is None or group['passed'] != group['planned'] or value['samples'] != group['planned']:
        return (np.nan, np.nan, np.nan)
    return tuple(value[k] / divisor for k in ('median', 'minimum', 'maximum'))


def figure(data, algorithm, variants, output, phase):
    groups = [g for g in data['groups'] if g['suite'] == 'distributed' and
              g['mode'] == 'process-cluster' and g['algorithm'] == algorithm]
    sizes = sorted({int(g['dataset'].removeprefix('sparse-')) for g in groups})
    assert sizes == [10_000, 100_000, 1_000_000], sizes
    fig, axes = plt.subplots(1, 2, figsize=(11, 5.0), layout='constrained')
    for engine, (label, color) in STYLES.items():
        for variant, line, marker in zip(variants, ['-', '--'], ['o', 's']):
            series = {int(g['dataset'].removeprefix('sparse-')): g for g in groups
                      if g['engine'] == engine and g['variant'] == variant}
            for ax, key, divisor in zip(axes, ['seconds', 'execution_pss_bytes'], [1, 2**20]):
                values = np.array([metric(series[n], key, divisor) for n in sizes])
                ax.errorbar(sizes, values[:, 0],
                            yerr=[values[:, 0] - values[:, 1], values[:, 2] - values[:, 0]],
                            label=f'{label} / {LABELS[variant]}', color=color, linestyle=line,
                            marker=marker, markersize=4, linewidth=1.5, capsize=3, alpha=.9)
    axes[0].set_ylabel('End-to-end seconds (log scale)')
    axes[0].set_yscale('log')
    axes[1].set_ylabel('Sampled execution peak PSS (MiB)')
    for ax in axes:
        ax.set_xscale('log')
        ax.set_xticks(sizes, [f'{n:,}' for n in sizes])
        ax.set_xlabel('Vertices')
        ax.grid(True, which='major', alpha=.2)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='outside lower center', ncol=3, frameon=False)
    algorithm_label = 'PageRank' if algorithm == 'pagerank' else 'WCC'
    phase_label = 'matched fusion supplement' if phase == 'fusion' else 'primary matrix'
    fig.suptitle(f'{algorithm_label} · {phase_label} · two process workers\n'
                 'Morrobay Linux VM · 8 vCPUs / 32 GiB · median and full range, 3/3 groups\n'
                 'Mean out-degree 8 · component block 1,024 · Banda kernels run on the driver', fontsize=11)
    for extension in ('png', 'svg'):
        path = output / f'{phase}-{algorithm}-scaling.{extension}'
        fig.savefig(path, dpi=180)
        if extension == 'svg':
            path.write_text('\n'.join(line.rstrip() for line in path.read_text().splitlines()) + '\n')
    plt.close(fig)


def compact_table(data, dataset, mode):
    lines = ['| Algorithm | Path | Method | Passes | Seconds | PSS MiB | RSS MiB | Cgroup MiB |',
             '| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |']
    for group in data['groups']:
        if group['dataset'] != dataset or group['mode'] != mode:
            continue
        values = []
        for key, divisor in [('seconds', 1), ('execution_pss_bytes', 2**20),
                             ('execution_rss_bytes', 2**20), ('cgroup_peak_through_result_bytes', 2**20)]:
            middle, low, high = metric(group, key, divisor)
            values.append(f'{middle:.2f} [{low:.2f}, {high:.2f}]' if np.isfinite(middle) else 'unavailable')
        lines.append('| ' + ' | '.join([
            'PageRank' if group['algorithm'] == 'pagerank' else 'WCC', STYLES[group['engine']][0],
            LABELS[group['variant']], f"{group['passed']}/{group['planned']}", *values]) + ' |')
    return '\n'.join(lines)


def fusion_table(data):
    groups = {(g['dataset'], g['mode'], g['engine'], g['variant']): g for g in data['groups']
              if g['suite'] == 'distributed' and g['algorithm'] == 'wcc'}
    lines = ['| Vertices | Path | Unfused seconds | Fused seconds | Fused / unfused median time | Unfused PSS MiB | Fused PSS MiB |',
             '| ---: | --- | ---: | ---: | ---: | ---: | ---: |']
    ratios = []
    for n in [10_000, 100_000, 1_000_000]:
        for engine, (label, _) in STYLES.items():
            pair = [groups[(f'sparse-{n}', 'process-cluster', engine, variant)]
                    for variant in ['optimized', 'fused']]
            timing = [metric(g, 'seconds')[0] for g in pair]
            memory = [metric(g, 'execution_pss_bytes', 2**20)[0] for g in pair]
            ratio = timing[1] / timing[0]
            def fmt(value): return f'{value:.3f}' if np.isfinite(value) else 'unavailable'
            lines.append('| ' + ' | '.join([f'{n:,}', label, *map(fmt, timing), fmt(ratio), *map(fmt, memory)]) + ' |')
            ratios.append(dict(vertices=n, path=label, fused_to_unfused_seconds_ratio=ratio if np.isfinite(ratio) else None,
                               unfused_seconds=timing[0] if np.isfinite(timing[0]) else None,
                               fused_seconds=timing[1] if np.isfinite(timing[1]) else None))
    return '\n'.join(lines), ratios



def chain_table(csv_path):
    with csv_path.open() as stream:
        rows = [r for r in csv.DictReader(stream) if r['suite'] == 'chain-diagnostic']
    assert len(rows) == 6
    lines = ['| Path | Method | Outcome | Time boundary | Seconds | PSS MiB |',
             '| --- | --- | --- | --- | ---: | ---: |']
    for row in sorted(rows, key=lambda r: (r['engine'], r['variant'])):
        passed = row['outcome'] == 'passed'
        seconds = row['seconds'] if passed else row.get('elapsed_until_error_seconds')
        memory = row['execution_pss_bytes']
        def fmt(value, divisor=1):
            return f'{float(value) / divisor:.3f}' if value else 'unavailable'
        lines.append('| ' + ' | '.join([STYLES[row['engine']][0], LABELS[row['variant']], row['outcome'],
            'completed output' if passed else 'until failure/cap', fmt(seconds), fmt(memory, 2**20)]) + ' |')
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--primary', type=Path, required=True)
    parser.add_argument('--fusion', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    primary, fusion = (json.loads(p.read_text()) for p in [args.primary, args.fusion])
    assert primary['planned_cells'] == 150 and fusion['planned_cells'] == 78
    assert not primary['integrity_errors'] and not fusion['integrity_errors']
    args.output.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({'font.size': 10, 'svg.fonttype': 'none'})
    for algorithm in ['pagerank', 'wcc']:
        figure(primary, algorithm, ['reference', 'optimized'], args.output, 'primary')
    figure(fusion, 'wcc', ['optimized', 'fused'], args.output, 'fusion')
    for name, data in [('primary', primary), ('fusion', fusion)]:
        for mode in ['local', 'process-cluster']:
            (args.output / f'{name}-100k-{mode}.md').write_text(compact_table(data, 'sparse-100000', mode) + '\n')
    for phase, path in [('primary', args.primary), ('fusion', args.fusion)]:
        (args.output / f'{phase}-chain-diagnostic.md').write_text(chain_table(path.parent / 'cells.csv') + '\n')
    table, ratios = fusion_table(fusion)
    (args.output / 'fusion-comparison.md').write_text(table + '\n')
    (args.output / 'fusion-ratios.json').write_text(json.dumps(ratios, indent=2) + '\n')


if __name__ == '__main__':
    main()
