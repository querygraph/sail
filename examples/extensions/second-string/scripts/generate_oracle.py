"""Generate compact fixtures by calling the pinned, compiled Scala reference."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import random
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

REFERENCE_COMMIT = "a35db39fa8e9b65db2d201a45b86d11a6ca34b98"
SEED = 0x5EC0AD
MAX_FIXTURE_BYTES = 1_048_576
type Parameter = int | float | str
type Expected = float | str

FUNCTIONS = (
    "ss_jaccard",
    "ss_cosine",
    "ss_sorensen_dice",
    "ss_overlap_coefficient",
    "ss_braun_blanquet",
    "ss_monge_elkan",
    "ss_levenshtein",
    "ss_lcs_similarity",
    "ss_jaro",
    "ss_jaro_winkler",
    "ss_needleman_wunsch",
    "ss_smith_waterman",
    "ss_affine_gap",
    "ss_soundex",
    "ss_refined_soundex",
    "ss_double_metaphone",
)
JAVA_OPENS = (
    "sun.nio.ch",
    "java.lang",
    "java.nio",
    "java.lang.invoke",
    "java.util",
    "sun.security.action",
    "java.io",
)


@dataclass(frozen=True, slots=True)
class Pair:
    left: str
    right: str


@dataclass(frozen=True, slots=True)
class Definition:
    function: str
    parameters: tuple[Parameter, ...] = ()


@dataclass(frozen=True, slots=True)
class Case:
    id: str
    definition: Definition
    pair: int


@dataclass(frozen=True, slots=True)
class Pin:
    path: Path
    size: int
    sha256: str


@dataclass(frozen=True, slots=True)
class Command:
    argv: tuple[str, ...]
    returncode: int
    stdout: Path
    stderr: Path


@dataclass(frozen=True, slots=True)
class Parsed:
    metadata: tuple[tuple[str, str], ...]
    uppercase: tuple[tuple[int, str], ...]
    results: tuple[tuple[str, Expected], ...]


@dataclass(frozen=True, slots=True)
class Config:
    java: Path
    javac: Path
    classpath_file: Path
    reference: Path
    work_dir: Path
    output: Path


def pin(path: Path) -> Pin:
    payload = path.read_bytes()
    return Pin(path.resolve(), len(payload), hashlib.sha256(payload).hexdigest())


def encoded(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def decoded(value: str) -> str:
    return base64.b64decode(value, validate=True).decode("utf-8")


def git(reference: Path, *arguments: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(reference), *arguments],
        text=True,
        stderr=subprocess.PIPE,
    ).strip()


def source_pins(reference: Path) -> tuple[Pin, ...]:
    paths = sorted((reference / "src/main/scala").rglob("*.scala"))
    paths += [reference / "build.sbt", reference / "project/build.properties"]
    return tuple(pin(path) for path in paths)


def corpus() -> tuple[Pair, ...]:
    pairs = [
        Pair("", ""),
        Pair("", "hello"),
        Pair("hello", ""),
        Pair("", " \t"),
        Pair(" \t", "alpha"),
        Pair(" \t\n", "\r "),
        Pair("a b c", "a b d"),
        Pair("a b", "a b c"),
        Pair("a a a b", "a b"),
        Pair("a a b", "a c c"),
        Pair("hello  world", "hello world"),
        Pair("hello\tworld", "hello\nworld"),
        Pair("alpha,beta", "alpha beta"),
        Pair("Alpha beta", "alpha Beta"),
        Pair("abcd", "abce"),
        Pair("abc", "xyz"),
        Pair("ab", "abc"),
        Pair("a b", "a c"),
        Pair("aaaa", "aa"),
        Pair("a", "b"),
        Pair("stephen smyth", "steven smith"),
        Pair("alpha beta", "alpha gamma"),
        Pair("alpha alpha beta", "alpha gamma"),
        Pair("beta alpha", "alpha beta"),
        Pair("martha", "marhta"),
        Pair("DWAYNE", "DUANE"),
        Pair("DIXON", "DICKSONX"),
        Pair("kitten", "sitting"),
        Pair("spark", "spork"),
        Pair("abc", "ac"),
        Pair("café", "cafe"),
        Pair("cafe\u0301", "café"),
        Pair("résumé", "resume"),
        Pair("Müller", "Mueller"),
        Pair("東京", "東京"),
        Pair("東京", "大阪"),
        Pair("東京 都", "東京"),
        Pair("😀", "😀"),
        Pair("😀", "😁"),
        Pair("hello 世界", "hello world"),
        Pair("John Смит", "John Smith"),
        Pair("alpha\u200bbeta", "alpha beta"),
        Pair("alpha\u2060beta", "alpha beta"),
        Pair("北京大学", "北京大学院"),
        Pair("😀a", "😁a"),
        Pair("😀😁", "😁😀"),
        Pair("a😀a", "a😁a"),
        Pair("𐐀", "𐐨"),
        Pair("İıſß", "IiSs"),
        Pair("Robert", "Rupert"),
        Pair("Rubin", "Robert"),
        Pair("Ashcraft", "Ashcroft"),
        Pair("Pfister", "Tymczak"),
        Pair("Smith", "Schmidt"),
        Pair("Schneider", "Snyder"),
        Pair("Washington", "Jackson"),
        Pair("O'Brien", "Obrien"),
        Pair("A-İ-ı-ſ-Z", "AIISZ"),
        Pair("1234", "!@#$"),
        Pair("ÆØŁ", "AEOL"),
        Pair("аβ中", "ABC"),
        Pair("a\x00b", "ab"),
        Pair("a\r\nb", "a b"),
        Pair("abcabc", "abc"),
    ]
    delimiters = (
        *range(0x09, 0x0E),
        *range(0x1C, 0x21),
        0x1680,
        *range(0x2000, 0x2007),
        *range(0x2008, 0x200B),
        0x2028,
        0x2029,
        0x205F,
        0x3000,
    )
    others = (0x85, 0xA0, 0x180E, 0x2007, 0x200B, 0x202F, 0x2060, 0xFEFF)
    pairs += [Pair(f"a{chr(unit)}b", "a b") for unit in (*delimiters, *others)]
    names = (
        "McDonald",
        "Macdonald",
        "Gough",
        "Laugh",
        "Knight",
        "Wright",
        "Xavier",
        "Jose",
        "José",
        "Jorge",
        "San Jacinto",
        "Thomas",
        "Thompson",
        "Schwarz",
        "Wicz",
        "Witz",
        "Szczepan",
        "Cheng",
        "Campbell",
        "Sugar",
        "Allen",
        "A",
        "BB",
        "Pf",
        "Cz",
        "Accident",
        "Wheeler",
        "Schaeffer",
        "Bacher",
    )
    pairs += [Pair(name, name.swapcase()) for name in names]
    rng = random.Random(SEED)
    alphabet = "abcdXYZ012 ,.-\t\n" + "éıſ中東😀😁\u0301\u00a0\u2003\u200b"
    while len(pairs) < 300:
        left = "".join(rng.choice(alphabet) for _ in range(rng.randrange(0, 18)))
        mode = rng.randrange(4)
        if mode == 0:
            right = left
        elif mode == 1:
            right = left[::-1]
        elif mode == 2:
            right = left + rng.choice(alphabet)
        else:
            right = "".join(rng.choice(alphabet) for _ in range(rng.randrange(0, 18)))
        pairs.append(Pair(left, right))
    return tuple(pairs)


def cases(pairs: tuple[Pair, ...]) -> tuple[Case, ...]:
    configured: list[Definition] = []
    for function in FUNCTIONS[:5]:
        configured += [Definition(function, (n,)) for n in (1, 2, 3)]
    configured += [
        Definition("ss_jaro_winkler", (0.2, 6)),
        Definition("ss_needleman_wunsch", (2, -2, -1)),
        Definition("ss_smith_waterman", (3, -1, -2)),
        Definition("ss_affine_gap", (-2, -3, -2)),
    ]
    for inner in ("jaro_winkler", "jaro", "levenshtein", "needleman_wunsch", "smith_waterman"):
        configured += [Definition("ss_monge_elkan", (inner, n)) for n in (0, 1, 2, 3)]
    definitions = [
        (index, Definition(function)) for index in range(len(pairs)) for function in FUNCTIONS
    ]
    definitions += [(index, definition) for index in range(64) for definition in configured]
    return tuple(
        Case(f"c{number:05d}", definition, index)
        for number, (index, definition) in enumerate(definitions)
    )


def write_input(path: Path, pairs: tuple[Pair, ...], records: tuple[Case, ...]) -> None:
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        for case in records:
            pair = pairs[case.pair]
            params = ",".join(str(value) for value in case.definition.parameters)
            stream.write(
                f"{case.id}\t{case.definition.function}\t{encoded(pair.left)}\t"
                f"{encoded(pair.right)}\t{params}\n",
            )


def execute(argv: tuple[str, ...], work: Path, label: str, stdin: Path | None = None) -> Command:
    stdout = work / f"{label}.stdout"
    stderr = work / f"{label}.stderr"
    with stdout.open("xb") as out, stderr.open("xb") as err:
        if stdin is None:
            result = subprocess.run(argv, stdout=out, stderr=err, timeout=180, check=False)
        else:
            with stdin.open("rb") as inp:
                result = subprocess.run(
                    argv, stdin=inp, stdout=out, stderr=err, timeout=180, check=False
                )
    command = Command(argv, result.returncode, stdout, stderr)
    if command.returncode != 0:
        raise RuntimeError(f"{label} failed ({command.returncode}); see {stderr}")
    return command


def parse_output(path: Path, records: tuple[Case, ...]) -> Parsed:
    metadata: dict[str, str] = {}
    uppercase: list[tuple[int, str]] = []
    results: list[tuple[str, Expected]] = []
    expected = {case.id: case for case in records}
    seen: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        fields = line.split("\t")
        if len(fields) != 3:
            raise ValueError(f"Malformed oracle output: {line[:120]}")
        first, kind, value = fields
        if first == "#meta":
            if kind in metadata:
                raise ValueError(f"Repeated metadata {kind}")
            metadata[kind] = decoded(value)
        elif first == "#upper":
            uppercase.append((int(kind), value))
        else:
            if first not in expected or first in seen:
                raise ValueError(f"Unknown or repeated oracle case {first}")
            unary = expected[first].definition.function in FUNCTIONS[-3:]
            if kind == "string" and unary:
                answer: Expected = decoded(value)
            elif kind == "score" and not unary:
                answer = float(value)
                if not math.isfinite(answer):
                    raise ValueError(f"Nonfinite reference answer for {first}: {value}")
            else:
                raise ValueError(f"Wrong result kind for {first}: {kind}")
            seen.add(first)
            results.append((first, answer))
    if seen != expected.keys():
        raise ValueError("Oracle omitted cases")
    if len({unit for unit, _ in uppercase}) != len(uppercase):
        raise ValueError("Repeated uppercase units")
    if not uppercase or any(
        len(letter) != 1 or not "A" <= letter <= "Z" for _, letter in uppercase
    ):
        raise ValueError("Invalid Java uppercase mapping")
    return Parsed(tuple(sorted(metadata.items())), tuple(uppercase), tuple(results))


def origin_pins(parsed: Parsed) -> tuple[Pin, ...]:
    paths: set[Path] = set()
    for key, value in parsed.metadata:
        if key.startswith("java."):
            continue
        uri = urlparse(value)
        if uri.scheme != "file" or uri.netloc:
            raise ValueError(f"Unexpected class origin {key}: {value}")
        path = Path(unquote(uri.path))
        if path.is_dir():
            paths.update(path.rglob("*.class"))
        else:
            paths.add(path)
    return tuple(pin(path) for path in sorted(paths))


def pin_json(value: Pin) -> dict[str, str | int]:
    return {"path": str(value.path), "bytes": value.size, "sha256": value.sha256}


def command_json(value: Command) -> dict[str, object]:
    return {
        "argv": value.argv,
        "returncode": value.returncode,
        "stdout": pin_json(pin(value.stdout)),
        "stderr": pin_json(pin(value.stderr)),
    }


def generate(config: Config) -> None:
    if config.output.exists() or config.work_dir.exists():
        raise FileExistsError("Fixture output and work directory must both be fresh")
    head = git(config.reference, "rev-parse", "HEAD")
    if head != REFERENCE_COMMIT or git(config.reference, "status", "--porcelain"):
        raise ValueError("Reference must be clean at the exact pinned commit")
    source_before = source_pins(config.reference)
    harness = Path(__file__).with_name("Oracle.java")
    helper_before = (pin(harness), pin(Path(__file__)), pin(config.classpath_file))
    classpath = config.classpath_file.read_text(encoding="utf-8").strip()
    if "\n" in classpath or not classpath.startswith(str(config.reference / "target")):
        raise ValueError("Expected exactly one exported reference classpath line")
    if any(not Path(item).exists() for item in classpath.split(os.pathsep)):
        raise ValueError("Classpath contains missing entries")
    config.work_dir.mkdir(parents=True)
    classes = config.work_dir / "classes"
    classes.mkdir()
    compiled = execute(
        (str(config.javac), "-encoding", "UTF-8", "-d", str(classes), str(harness)),
        config.work_dir,
        "javac",
    )
    pairs = corpus()
    records = cases(pairs)
    request = config.work_dir / "cases.tsv"
    write_input(request, pairs, records)
    arguments = (
        str(config.java),
        "-Xmx512m",
        *(f"--add-opens=java.base/{package}=ALL-UNNAMED" for package in JAVA_OPENS),
        "-cp",
        f"{classes}{os.pathsep}{classpath}",
        "Oracle",
    )
    evaluated = execute(arguments, config.work_dir, "oracle", request)
    parsed = parse_output(evaluated.stdout, records)
    actual_origins = origin_pins(parsed)
    if (
        source_before != source_pins(config.reference)
        or git(config.reference, "rev-parse", "HEAD") != head
    ):
        raise ValueError("Reference source changed while the oracle ran")
    if git(config.reference, "status", "--porcelain") or helper_before != (
        pin(harness),
        pin(Path(__file__)),
        pin(config.classpath_file),
    ):
        raise ValueError("Oracle helper or reference cleanliness changed")
    manifest = {
        "commit": head,
        "source": [pin_json(value) for value in source_before],
        "loaded_class_origins": [pin_json(value) for value in actual_origins],
        "java": dict(parsed.metadata),
    }
    fingerprint = hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    results = dict(parsed.results)
    fixture = {
        "reference": {
            "repository": "https://github.com/SemyonSinchenko/spark-second-string",
            "commit": head,
            "fingerprint_sha256": fingerprint,
            "generated_utc": datetime.now(UTC).isoformat(),
            "seed": SEED,
            "pair_count": len(pairs),
            "case_count": len(records),
            "numeric_absolute_tolerance": 1e-12,
            "evaluation": "compiled Scala companion methods, not Spark jobs",
            "null_scope": "non-null oracle inputs; null propagation is tested at the Sail caller",
            "metadata": manifest,
            "helpers": [pin_json(value) for value in helper_before],
            "commands": [command_json(compiled), command_json(evaluated)],
        },
        "upper_ascii": [{"unit": unit, "uppercase": upper} for unit, upper in parsed.uppercase],
        "pairs": [{"left": pair.left, "right": pair.right} for pair in pairs],
        "cases": [
            {
                "id": case.id,
                "function": case.definition.function,
                "pair": case.pair,
                "parameters": case.definition.parameters,
                "expected": results[case.id],
            }
            for case in records
        ],
    }
    payload = (
        json.dumps(fixture, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode()
    if len(payload) > MAX_FIXTURE_BYTES:
        raise ValueError(f"Fixture exceeds {MAX_FIXTURE_BYTES} bytes: {len(payload)}")
    config.output.parent.mkdir(parents=True, exist_ok=True)
    with config.output.open("xb") as output:
        output.write(payload)
    print(
        json.dumps(
            {
                "outcome": "generated_compiled_reference_oracle",
                "fixture": pin_json(pin(config.output)),
                "cases": len(records),
                "pairs": len(pairs),
                "reference_fingerprint": fingerprint,
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("java", "javac", "classpath-file", "reference", "work-dir", "output"):
        parser.add_argument(f"--{option}", required=True, type=Path)
    args = parser.parse_args()
    generate(
        Config(
            args.java, args.javac, args.classpath_file, args.reference, args.work_dir, args.output
        )
    )


if __name__ == "__main__":
    main()
