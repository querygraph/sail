import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).parents[1] / f'{name}.py')
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


class ReviewToolsTest(unittest.TestCase):
    def test_wheel_payload_checks_installed_native_bytes(self):
        check = module('check_compatibility')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wheel = root / 'fixture.whl'
            with zipfile.ZipFile(wheel, 'w') as z:
                z.writestr('fixture/native.so', b'native-bytes')
            (root / 'fixture').mkdir()
            installed = root / 'fixture/native.so'
            installed.write_bytes(b'native-bytes')
            with patch.object(check.sysconfig, 'get_paths', return_value={'purelib': str(root)}):
                self.assertIn('fixture.whl', check.wheel_inventory([wheel]))
                installed.write_bytes(b'different-native-bytes')
                with self.assertRaisesRegex(ValueError, 'payload mismatch'):
                    check.wheel_inventory([wheel])

    def test_evidence_preserves_failures_and_hashes_and_excludes_binaries(self):
        bundle = module('package_review_evidence')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = b'FAILED /Users/alexy/src at morrobay 192.168.4.63\n'
            (root/'failed.log').write_bytes(raw)
            (root/'sail').write_bytes(b'\x00binary')
            (root/'._sidecar.py').write_bytes(b'\x00\x05\x16\x07')
            files, entries, excluded = bundle.collect([('gate', root)])
            self.assertIn(b'FAILED', files['gate/failed.log'])
            self.assertNotIn(b'alexy', files['gate/failed.log'])
            self.assertNotIn(b'192.168.4.63', files['gate/failed.log'])
            self.assertEqual(entries[0]['original_sha256'], bundle.sha(raw))
            self.assertEqual(excluded, ['gate/._sidecar.py', 'gate/sail'])
            (root/'secret.log').write_text('-----BEGIN OPENSSH PRIVATE KEY-----')
            with self.assertRaisesRegex(ValueError, 'credential'):
                bundle.collect([('gate', root)])


if __name__ == '__main__':
    unittest.main()
