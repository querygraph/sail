"""Validate published benchmark artifacts and local documentation references."""
import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tarfile


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    repo=args.repo.resolve()
    source=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
    status=subprocess.check_output(['git','status','--porcelain'],cwd=repo,text=True).strip()
    assert not status,status
    base=repo/'docs/development/extensions'
    artifacts=base/'pecan-nutmeg-benchmark'
    report=base/'pecan-nutmeg-benchmark.md'
    text=report.read_text()
    assert not re.search(r'<!-- INSERT|\bTODO\b|\bTBD\b',text)
    documentation=[report, repo/'examples/extensions/benchmarks/TUTORIAL.md',
                   repo/'examples/extensions/benchmarks/README.md',
                   repo/'examples/extensions/graph-algorithms/README.md',
                   repo/'examples/extensions/graph-algorithms/TESTING.md',
                   repo/'examples/extensions/README.md', base/'portable-graph-plan.md',
                   base/'portable-graph-validation.md', artifacts/'README.md']
    local_links=0
    for doc in documentation:
        for link in re.findall(r'\]\(([^)]+)\)',doc.read_text()):
            if link.startswith(('https:', 'http:', '#','mailto:')): continue
            path=link.split('#')[0]
            assert (doc.parent/path).resolve().exists(),(doc.relative_to(repo),link)
            local_links+=1
    phases={}
    for phase,total in [('primary',150),('fusion',78),('constrained',150)]:
        directory=artifacts/phase
        summary=json.loads((directory/'summary.json').read_text())
        assert summary['planned_cells']==total
        assert not summary['integrity_errors']
        with (directory/'cells.csv').open() as stream: cells=list(csv.DictReader(stream))
        assert len(cells)==total
        assert Counter(c['outcome'] for c in cells)==summary['outcomes']
        assert sum(g['planned'] for g in summary['groups'])==total
        assert sum(g['passed'] for g in summary['groups'])==summary['outcomes'].get('passed',0)
        for g in summary['groups']:
            for value in g['metrics'].values():
                if value is None:continue
                assert value['samples']<=g['passed']
                assert value['minimum']<=value['median']<=value['maximum']
        phases[phase]={'sources':summary['sources'],'outcomes':summary['outcomes'],'planned_cells':total}
    proof=artifacts/'proof'
    bundle=json.loads((proof/'bundle.json').read_text())
    archive=proof/'evidence.tar.gz'
    assert sha(archive)==bundle['archive_sha256']
    scan=json.loads((proof/'credential-scan.json').read_text())
    assert scan['outcome']=='passed' and not scan['matches']
    assert scan['archive_sha256']==bundle['archive_sha256']
    manifest=json.loads((proof/'manifest.json').read_text())
    with tarfile.open(archive,'r:gz') as tar:
        assert tar.extractfile('manifest.json').read()==(proof/'manifest.json').read_bytes()
        for item in manifest['included']:
            raw=tar.extractfile(item['path']).read()
            assert len(raw)==item['bytes']
            assert hashlib.sha256(raw).hexdigest()==item['sha256'],item['path']
    assert len(manifest['included'])==bundle['included_files']
    end=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
    assert source==end
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo,text=True).strip()
    receipt=dict(recorded_utc=datetime.now(timezone.utc).isoformat(),source_sha=source,ending_sha=end,
                 outcome='passed',local_links_checked=local_links,phases=phases,
                 archive_sha256=bundle['archive_sha256'],evidence_files_verified=bundle['included_files'],
                 report_sha256=sha(report),boundary='Artifact integrity, source immutability, outcome counts and documentation links; execution gates and independent result audits are separately identified in the report.')
    args.output.write_text(json.dumps(receipt,indent=2)+'\n')
    print('BENCHMARK_DELIVERY_GATE',source,'PASSED')


if __name__=='__main__':main()
