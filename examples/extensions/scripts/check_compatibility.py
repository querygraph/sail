#!/usr/bin/env python3
"""Run unchanged installed wheels against multiple independently identified hosts.

The host manifest is an array of {source_commit, binary, binary_sha256}. Source
provenance must come from the build receipt; an executable hash cannot establish
its source by itself. This script never builds or installs a wheel.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import sysconfig
import zipfile


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def wheel_inventory(wheels):
    """Require the installed package payload to match each supplied wheel exactly."""
    site = Path(sysconfig.get_paths()['purelib'])
    result = {}
    for path in wheels:
        members = {}
        with zipfile.ZipFile(path) as wheel:
            for name in wheel.namelist():
                if name.endswith('/') or '.dist-info/' in name:
                    continue
                relative = Path(name)
                if relative.is_absolute() or '..' in relative.parts or '.data' in name:
                    raise ValueError(f'unsupported wheel member layout: {name}')
                installed = site / relative
                expected = hashlib.sha256(wheel.read(name)).hexdigest()
                if not installed.is_file() or digest(installed) != expected:
                    raise ValueError(f'installed wheel payload mismatch: {name}')
                members[name] = expected
        if not members:
            raise ValueError(f'empty wheel payload: {path}')
        result[Path(path).name] = {'wheel_sha256': digest(path), 'installed_members': members}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--hosts', type=Path, required=True)
    parser.add_argument('--wheel', type=Path, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    hosts = json.loads(args.hosts.read_text())
    if len({h['source_commit'] for h in hosts}) < 2:
        parser.error('at least two distinct host source revisions are required')
    args.output.mkdir(parents=True, exist_ok=False)
    before = wheel_inventory(args.wheel)
    repo = Path(__file__).resolve().parents[3]
    tests = repo / 'examples/extensions/tests'
    result = dict(recorded_utc=datetime.now(timezone.utc).isoformat(),
                  scope='Same platform, Python and API/DataFusion/Arrow tuple; no ABI-range claim',
                  python=sys.version, wheels=before, hosts=[], status='running',
                  harness_sha256=digest(__file__),
                  test_sources={str(p.relative_to(repo)): digest(p) for p in sorted(tests.glob('*.py'))})
    failed = False
    try:
        for i, host in enumerate(hosts):
            binary = Path(host['binary']).resolve()
            if digest(binary) != host['binary_sha256']:
                raise ValueError(f'host artifact mismatch: {binary}')
            record = dict(host, runs=[])
            result['hosts'].append(record)
            for mode in ['local', 'local-cluster', 'process-cluster']:
                name = f'host-{i}-{mode}'
                command = [sys.executable, '-m', 'pytest', str(tests),
                           '--sail-binary', str(binary), '--execution-mode', mode,
                           '--basetemp', str(args.output.resolve() / name), '-q']
                with (args.output / f'{name}.log').open('w') as log:
                    run = subprocess.run(command, cwd=repo, stdout=log, stderr=subprocess.STDOUT)
                record['runs'].append(dict(mode=mode, command=command, exit_code=run.returncode,
                                           log=f'{name}.log'))
                failed |= run.returncode != 0
            if digest(binary) != host['binary_sha256']:
                raise ValueError(f'host changed during qualification: {binary}')
        result['wheels_unchanged'] = wheel_inventory(args.wheel) == before
        if not result['wheels_unchanged']:
            raise ValueError('wheel bytes changed during qualification')
        result['status'] = 'failed' if failed else 'passed'
    except BaseException as error:
        result['status'] = 'failed'
        result['error'] = str(error)
        raise
    finally:
        result['completed_utc'] = datetime.now(timezone.utc).isoformat()
        (args.output / 'receipt.json').write_text(json.dumps(result, indent=2) + '\n')
    return int(failed)


if __name__ == '__main__':
    sys.exit(main())
