#!/usr/bin/env python3
"""Export selected text evidence, retaining failures and checksums, without binaries.

This is a redacted derivative, not a byte-identical copy of original receipts.
Both original and exported SHA-256 values are recorded. No source is modified.
"""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import io
import ipaddress
import json
from pathlib import Path
import re
import tarfile

TEXT_SUFFIXES = {'.json', '.jsonl', '.log', '.md', '.txt', '.py', '.sh', '.toml', '.rs'}
SECRET = re.compile(r'-----BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY-----|\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|AKIA[A-Z0-9]{16})\b')


def sha(data):
    return hashlib.sha256(data).hexdigest()


def redact(text):
    text = re.sub(r'/(Users|home)/[^/\s"\']+', r'/\1/REDACTED', text)
    text = re.sub(r'\b(?:Capitola|capitola)(?:\.local)?\b', 'review-host-a', text)
    text = re.sub(r'\b(?:Morrobay|morrobay)(?:\.local)?\b', 'review-host-b', text)
    text = re.sub(r'\b[\w.-]+@(review-host-[ab])\b', r'REDACTED@\1', text)
    def address(match):
        value = match.group()
        try:
            ip = ipaddress.ip_address(value)
        except ValueError:
            return value
        if ip.is_private and not ip.is_loopback and not ip.is_unspecified:
            # Retain distinct evidence-host identity without exposing LAN addresses.
            known = {'192.168.4.61': '192.0.2.1', '192.168.4.63': '192.0.2.2'}
            return known.get(value, '192.0.2.254')
        return value
    return re.sub(r'\b(?:\d{1,3}\.){3}\d{1,3}\b', address, text)


def collect(roots):
    files = {}; entries = []; excluded = []
    for label, root in roots:
        for path in sorted(root.rglob('*')):
            if path.is_symlink():
                continue
            if not path.is_file():
                continue
            name = f'{label}/{path.relative_to(root).as_posix()}'
            if path.name.startswith('._') or path.suffix not in TEXT_SUFFIXES or '__pycache__' in path.parts:
                excluded.append(name)
                continue
            raw = path.read_bytes()
            if len(raw) > 32 * 1024 * 1024:
                raise ValueError(f'text evidence exceeds 32 MiB: {name}')
            text = raw.decode('utf-8')
            if SECRET.search(text):
                raise ValueError(f'potential credential in {name}; review before export')
            data = redact(text).encode()
            files[name] = data
            entries.append(dict(path=name, original_sha256=sha(raw), exported_sha256=sha(data),
                                original_bytes=len(raw), exported_bytes=len(data), redacted=data != raw))
    return files, entries, excluded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', action='append', required=True, help='label=directory')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    roots=[]
    for value in args.source:
        label, directory = value.split('=', 1)
        if not re.fullmatch(r'[a-zA-Z0-9_-]+', label):
            parser.error('source labels must be simple directory names')
        root = Path(directory).resolve()
        if not root.is_dir():
            parser.error(f'missing evidence directory: {root}')
        roots.append((label, root))
    if len({label for label, _ in roots}) != len(roots):
        parser.error('duplicate source label')
    if args.output.exists():
        parser.error('refusing to overwrite an evidence bundle')
    files, entries, excluded = collect(roots)
    manifest = dict(schema_version=1, recorded_utc=datetime.now(timezone.utc).isoformat(),
                    scope='Redacted text evidence; executable and wheel bytes excluded, artifact hashes retained in receipts',
                    redactions=['home directory account names', 'two review hostnames and SSH accounts',
                                'private non-loopback IPv4 addresses'],
                    verification='SHA256SUMS verifies exported bytes; manifest also records original hashes. Historical absolute paths are provenance, not bundle links.',
                    entries=entries, excluded_files=excluded)
    files['manifest.json'] = (json.dumps(manifest, indent=2)+'\n').encode()
    files['README.md'] = b'''# Review evidence bundle

This is a redacted derivative of recorded evidence, including failed candidates.
It is not an independent rerun or a claim that an old gate covers a newer commit.
Read each receipt's source revision and scope before using its verdict.

Verify after extraction with `shasum -a 256 -c SHA256SUMS`.
`manifest.json` maps original and exported hashes and discloses exclusions.
Historical absolute paths are retained in redacted form for provenance; navigate
using the manifest paths. Binaries and wheels are omitted; their recorded hashes
remain available. Credential-pattern detection is supplementary to manual review.
'''
    files['SHA256SUMS'] = ''.join(f'{sha(data)}  {name}\n' for name,data in sorted(files.items())).encode()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('xb') as target:
        with gzip.GzipFile(filename='', mode='wb', fileobj=target, mtime=0) as zipped:
            with tarfile.open(fileobj=zipped, mode='w') as archive:
                for name, data in sorted(files.items()):
                    info = tarfile.TarInfo(name); info.size=len(data); info.mode=0o644; info.mtime=0
                    archive.addfile(info, io.BytesIO(data))
    args.output.with_suffix(args.output.suffix+'.sha256').write_text(f'{sha(args.output.read_bytes())}  {args.output.name}\n')
    print(f'Exported {len(entries)} text records; excluded {len(excluded)} non-text files')


if __name__ == '__main__':
    main()
