"""Linux measurements for one trial in a fresh, private PID/cgroup container.

RSS sums count shared mappings more than once; PSS apportions them. The kernel
cgroup peak includes page cache and the entire container lifetime. Neither is
interchangeable with the graph extension's allocator accounting.
"""
import json
from pathlib import Path
import threading
import time


CGROUP = Path('/sys/fs/cgroup')


def read_text(path):
    try:
        return Path(path).read_text().strip()
    except (OSError, UnicodeError):
        return None


def counters(path):
    text = read_text(path)
    if text is None:
        return {}
    result = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[1].isdigit():
            result[fields[0].rstrip(':')] = int(fields[1])
    return result


def cgroup_snapshot():
    return {name: read_text(CGROUP / name) for name in (
        'memory.current', 'memory.peak', 'memory.max', 'memory.swap.max',
        'memory.events', 'memory.stat', 'cpu.max', 'cpu.stat', 'cpuset.cpus.effective')}


def cpu_ticks():
    text = read_text('/proc/stat')
    return list(map(int, text.splitlines()[0].split()[1:])) if text else None


def steal_fraction(before, after):
    if before is None or after is None:
        return None
    # user/nice include guest/guest_nice: do not count the latter twice.
    delta = [b - a for a, b in zip(before[:8], after[:8])]
    return delta[7] / sum(delta) if sum(delta) else 0.0


def process_memory():
    rows = []
    for path in Path('/proc').glob('[0-9]*'):
        status = counters(path / 'status')
        if 'VmRSS' not in status:
            continue
        rollup = counters(path / 'smaps_rollup') if (path / 'smaps_rollup').exists() else {}
        rows.append({'pid': int(path.name), 'name': read_text(path / 'comm'),
                     'rss_bytes': status['VmRSS'] * 1024,
                     'pss_bytes': rollup['Pss'] * 1024 if 'Pss' in rollup else None})
    return rows


class Sampler:
    def __init__(self, output, interval=0.05):
        self.output = Path(output)
        self.interval = interval
        self.phase = 'startup'
        self.generation = 0
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.peaks = {}
        self.first = {}
        self.count = 0
        self.phase_counts = {}
        self.error = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def _run(self):
        try:
            with self.output.open('w') as stream:
                while not self.stop.is_set():
                    with self.lock:
                        phase, generation = self.phase, self.generation
                    started = time.monotonic()
                    processes = process_memory() if Path('/proc/stat').exists() else []
                    current = read_text(CGROUP / 'memory.current')
                    with self.lock:
                        if generation != self.generation:
                            phase = 'transition'
                    row = {'scan_started_seconds': started, 'scan_finished_seconds': time.monotonic(), 'phase': phase,
                           'rss_bytes': sum(p['rss_bytes'] for p in processes) if processes else None,
                           'pss_bytes': sum(p['pss_bytes'] for p in processes)
                               if processes and all(p['pss_bytes'] is not None for p in processes) else None,
                           'pss_processes_available': sum(p['pss_bytes'] is not None for p in processes),
                           'cgroup_current_bytes': int(current) if current else None,
                           'processes': processes}
                    stream.write(json.dumps(row) + '\n')
                    stream.flush()
                    with self.lock:
                        self.count += 1
                        self.phase_counts[phase] = self.phase_counts.get(phase, 0) + 1
                        if phase == 'transition':
                            continue
                        self.first.setdefault(phase, row)
                        peak = self.peaks.setdefault(phase, {})
                        for key in ('rss_bytes', 'pss_bytes', 'cgroup_current_bytes'):
                            value = row[key]
                            if value is not None:
                                peak[key] = max(peak.get(key, 0), value)
                    self.stop.wait(self.interval)
        except BaseException as error:
            self.error = repr(error)

    def mark(self, phase):
        with self.lock:
            self.phase = phase
            self.generation += 1

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            self.error = 'sampler thread failed to stop'

    def receipt(self):
        return {'interval_seconds': self.interval, 'samples': self.count,
                'phase_peaks': self.peaks, 'phase_first_samples': self.first,
                'phase_sample_counts': self.phase_counts,
                'execution_sampled': bool(self.phase_counts.get('execute')),
                'thread_alive': self.thread.is_alive(),
                'error': self.error, 'scope': 'all visible processes: requires a private PID namespace',
                'rss_boundary': 'sum of contemporaneous process RSS, shared pages counted per process',
                'pss_boundary': 'sum of contemporaneous Linux proportional set size',
                'cgroup_boundary': 'container current/peak includes page cache and allocator overhead'}
