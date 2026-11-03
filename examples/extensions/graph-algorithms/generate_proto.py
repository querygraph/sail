"""Regenerate the packaged Python contract using protoc 36.1."""

from pathlib import Path
import subprocess
import tempfile


def main():
    package = Path(__file__).resolve().parent
    repository = package.parents[2]
    source = repository / "crates/sail-session/proto"
    with tempfile.TemporaryDirectory() as temporary:
        subprocess.run([
            "protoc", f"--proto_path={source}", f"--python_out={temporary}",
            str(source / "gf/utils/v1/utils.proto"),
        ], check=True)
        generated = (Path(temporary) / "gf/utils/v1/utils_pb2.py").read_text()
    # The wire namespace stays gf.utils.v1. Relocate only the Python import
    # namespace so generated message classes can also be imported/pickled.
    generated = generated.replace("'gf.utils.v1.utils_pb2'", "'pyspark_pecan.utils_pb2'")
    (package / "src/pyspark_pecan/utils_pb2.py").write_text(generated)


if __name__ == "__main__":
    main()
