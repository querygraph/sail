#!/usr/bin/env python3
"""Exercise declared-version refusal and the native memory-lease C boundary.

Every probe runs in a disposable process group with a timeout. Version fixtures
never supply native pointers; lease fixtures supply aligned C-layout records or
readable headers. Accepted records have live callbacks; rejected header-only
records end at a guard page. No invalid header pointer or forged function is used.
"""
import argparse
import ctypes
from datetime import datetime, timezone
import gc
import hashlib
import json
import mmap
import os
from pathlib import Path
import signal
import subprocess
import sys
from types import SimpleNamespace

from check_compatibility import wheel_inventory

VERSIONS = {
    'api-old': {'api_version': 0},
    'api-new': {'api_version': 2},
    'df-previous-minor': {'datafusion_version': '55.0.0'},
    'df-hypothetical-patch': {'datafusion_version': '55.1.1'},
    'df-next-major': {'datafusion_version': '56.0.0'},
    'arrow-previous-minor': {'arrow_version': '59.2.0'},
    'arrow-hypothetical-patch': {'arrow_version': '59.3.1'},
    'arrow-next-major': {'arrow_version': '60.0.0'},
}
LEASES = ['valid', 'version-old', 'version-new', 'size-short', 'size-long',
          'quota-mismatch', 'null-owner', 'guard-version', 'guard-size']


def lease_probe(case):
    from sail_nutmeg._native import BoundExtension
    if case.startswith('guard-'):
        # The import contract permits a readable header alone when rejected.
        # Put it immediately before an inaccessible page: any tail read faults
        # in this disposable child, rather than silently reading spare storage.
        allocation = mmap.mmap(-1, 2 * mmap.PAGESIZE, prot=mmap.PROT_READ | mmap.PROT_WRITE)
        address = ctypes.addressof(ctypes.c_char.from_buffer(allocation))
        libc = ctypes.CDLL(None, use_errno=True)
        libc.mprotect.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]
        libc.mprotect.restype = ctypes.c_int
        if libc.mprotect(address + mmap.PAGESIZE, mmap.PAGESIZE, 0) != 0:
            raise OSError(ctypes.get_errno(), 'mprotect failed')
        class Header(ctypes.Structure):
            _fields_ = [('version', ctypes.c_uint32), ('size', ctypes.c_uint32)]
        header = Header.from_buffer(allocation, mmap.PAGESIZE - ctypes.sizeof(Header))
        header.version = 2 if case == 'guard-version' else 1
        header.size = 8
        new_capsule = ctypes.pythonapi.PyCapsule_New
        new_capsule.restype = ctypes.py_object
        new_capsule.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
        capsule = new_capsule(ctypes.addressof(header), b'sail_native_memory_lease_v1', None)
        try:
            BoundExtension(1048576, capsule)
        except Exception as error:
            assert 'ABI mismatch' in str(error), str(error)
            print(str(error))
        else:
            raise AssertionError('header-only lease accepted')
        del capsule, header
        allocation.close()
        print(json.dumps({'case': case, 'readable_header_bytes': 8, 'guard_page': True}))
        return
    callback = ctypes.CFUNCTYPE(None, ctypes.c_void_p)
    events = []

    @callback
    def retain(_owner):
        events.append('retain')

    @callback
    def release(_owner):
        events.append('release')

    class Lease(ctypes.Structure):
        _fields_ = [('version', ctypes.c_uint32), ('size', ctypes.c_uint32),
                    ('bytes', ctypes.c_uint64), ('owner', ctypes.c_void_p),
                    ('retain', callback), ('release', callback)]

    owner = ctypes.c_uint64(42)
    quota = 1048576
    lease = Lease(1, ctypes.sizeof(Lease), quota, ctypes.addressof(owner), retain, release)
    if case == 'version-old':
        lease.version = 0
    elif case == 'version-new':
        lease.version = 2
    elif case == 'size-short':
        lease.size -= 8
    elif case == 'size-long':
        lease.size += 8
    elif case == 'quota-mismatch':
        lease.bytes -= 1
    elif case == 'null-owner':
        lease.owner = None
    new_capsule = ctypes.pythonapi.PyCapsule_New
    new_capsule.restype = ctypes.py_object
    new_capsule.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    # All backing objects and the capsule name remain live until after final drop.
    capsule = new_capsule(ctypes.addressof(lease), b'sail_native_memory_lease_v1', None)
    if case == 'valid':
        bound = BoundExtension(quota, capsule)
        assert events == ['retain'], events
        del bound
        gc.collect()
        assert events == ['retain', 'release'], events
    else:
        try:
            BoundExtension(quota, capsule)
        except Exception as error:
            expected = 'quota mismatch' if case in ('quota-mismatch', 'null-owner') else 'ABI mismatch'
            assert expected in str(error), str(error)
            print(str(error))
        else:
            raise AssertionError('invalid lease accepted')
        assert events == [], events
    print(json.dumps({'case': case, 'callback_events': events, 'lease_size': ctypes.sizeof(Lease)}))


def version_probe(case, binary, directory):
    tests = Path(__file__).resolve().parents[1] / 'tests'
    sys.path.insert(0, str(tests))
    from test_protocol import assert_loader_rejected, manifest
    request = SimpleNamespace(config=SimpleNamespace(getoption=lambda _name: binary))
    events = assert_loader_rejected(request, directory,
                                   [dict(manifest=manifest(**VERSIONS[case]))], 'build mismatch')
    assert events == [], 'loader must reject before binding or reading native capsules'
    print(json.dumps({'case': case, 'native_events': events}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sail-binary')
    parser.add_argument('--wheel', type=Path, action='append')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--timeout', type=int, default=90)
    parser.add_argument('--probe')
    args = parser.parse_args()
    args.output = args.output.resolve()
    if args.probe:
        kind, case = args.probe.split('/', 1)
        if kind == 'lease':
            lease_probe(case)
        else:
            version_probe(case, args.sail_binary, args.output)
        return 0
    if not args.sail_binary or not args.wheel or args.timeout <= 0:
        parser.error('--sail-binary, --wheel and a positive timeout are required')
    args.output.mkdir(parents=True, exist_ok=False)
    before = wheel_inventory(args.wheel)
    binary = Path(args.sail_binary).resolve()
    binary_hash = hashlib.file_digest(binary.open('rb'), 'sha256').hexdigest()
    receipt = dict(started_utc=datetime.now(timezone.utc).isoformat(),
                   binary=str(binary), binary_sha256=binary_hash, python=sys.version,
                   scope='Declared-version refusal and memory-lease callbacks; no cross-version execution claim',
                   wheels=before, probes=[], status='running',
                   harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   fixture_sources={name: hashlib.sha256((Path(__file__).resolve().parents[1] / 'tests' / name).read_bytes()).hexdigest()
                                    for name in ['conftest.py', 'test_protocol.py']})
    try:
        for probe in [*(f'manifest/{c}' for c in VERSIONS), *(f'lease/{c}' for c in LEASES)]:
            name = probe.replace('/', '-')
            directory = args.output / name
            directory.mkdir()
            command = [sys.executable, str(Path(__file__).resolve()), '--probe', probe,
                       '--sail-binary', str(binary), '--output', str(directory)]
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       text=True, start_new_session=True)
            timed_out = False
            try:
                output, _ = process.communicate(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    output, _ = process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    output, _ = process.communicate()
            (directory / 'probe.log').write_text(output)
            receipt['probes'].append(dict(probe=probe, command=command, exit_code=process.returncode,
                                          timed_out=timed_out))
            print(probe, process.returncode, 'timeout' if timed_out else '', flush=True)
        receipt['wheels_unchanged'] = wheel_inventory(args.wheel) == before
        receipt['binary_unchanged'] = hashlib.file_digest(binary.open('rb'), 'sha256').hexdigest() == binary_hash
        passed = all(p['exit_code'] == 0 and not p['timed_out'] for p in receipt['probes'])
        receipt['status'] = 'passed' if passed and receipt['wheels_unchanged'] and receipt['binary_unchanged'] else 'failed'
    finally:
        receipt['finished_utc'] = datetime.now(timezone.utc).isoformat()
        (args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    return int(receipt['status'] != 'passed')


if __name__ == '__main__':
    sys.exit(main())
