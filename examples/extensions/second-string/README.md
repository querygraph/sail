# Native Second String for Sail

An independent native extension wheel implementing all sixteen default functions
from [Spark Second String](https://github.com/SemyonSinchenko/spark-second-string)
at `a35db39fa8e9b65db2d201a45b86d11a6ca34b98`:

- Token similarities: Jaccard, Sørensen–Dice, overlap coefficient, cosine,
  Braun–Blanquet and Monge–Elkan.
- Character similarities: normalized Levenshtein, LCS, Jaro, Jaro–Winkler,
  Needleman–Wunsch, Smith–Waterman and affine gap.
- Phonetics: Soundex, Refined Soundex and primary Double Metaphone.

The wheel has no Sail crate dependency. Sail discovers its `pysail.extensions`
entry point, imports its native DataFusion scalar capsules, and evaluates Arrow
batches on the driver or workers. No JVM, Python UDF or Arrow Python UDF executes
these metrics. Java is used only to generate the source-comparison fixtures.

## Build and install

The current extension tuple is API 1, DataFusion 55.1.0 and Arrow 59.3.0.
Use a Python 3.12 environment with a shared library, Rust, and maturin:

```bash
python -m maturin build --release --locked \
  --manifest-path examples/extensions/second-string/Cargo.toml \
  --interpreter python --out /tmp/second-string-wheels
python -m pip install /tmp/second-string-wheels/*.whl
```

Install the same wheel on the server and every process worker. Start a compatible
Sail extension host with `SAIL_EXPERIMENTAL_EXTENSIONS=1`; the existing extension
[tutorial](../TUTORIAL.md) explains Python library setup and worker deployment.
The included static `sail-extension.json` also declares the same tuple for the
separate static-preflight candidate.

## Call from SQL or Spark Connect

Original SQL names and arities are retained: thirteen binary `ss_*` similarity
functions returning DOUBLE, and three unary phonetics returning STRING.
Similarity is case-sensitive. Either null input produces null.

```python
from pyspark.sql.connect.session import SparkSession
from sail_second_string import functions as ss

spark = SparkSession.builder.remote("sc://127.0.0.1:50051").create()
spark.sql("SELECT ss_jaro_winkler('MARTHA', 'MARHTA') AS score").show()
pairs = spark.createDataFrame([("abcd", "abce")], ["left", "right"])
pairs.select(ss.jaccard("left", "right", ngram_size=2).alias("score")).show()
```

Configurable Python helpers generate literal options for ten additional native
`ss_<metric>_with_options` functions. These preserve the upstream Scala DSL
parameters while keeping the original SQL functions' fixed arities. Options must
be literal constants. Helpers cover n-grams, Monge–Elkan's five inner metrics,
Jaro–Winkler prefix settings, alignment scores and affine penalties.

## Compatibility details

Character metrics and n-grams preserve Java UTF-16 indexing, including supplementary
characters. Whitespace tokenization follows Java `Character.isWhitespace` rather
than Python/Rust whitespace predicates. Phonetics use the Scala wrappers' ASCII
normalization; their Soundex variants differ from Commons Codec's encoders.
Double Metaphone uses the Commons Codec 1.21.0 primary-code rules with length four.

Empty and whitespace-only strings retain the source's metric-specific results.
Monge n-gram sums use a deterministic token order; comparisons allow `1e-12`
absolute error for floating accumulation against Java's unordered token set.
No normalization or silent algorithm substitutions are added.

## Verification

```bash
cargo fmt --all --manifest-path examples/extensions/second-string/Cargo.toml -- --check
cargo clippy --all-targets --manifest-path examples/extensions/second-string/Cargo.toml -- -D warnings
cargo test --release --all-targets --manifest-path examples/extensions/second-string/Cargo.toml
python -m pytest examples/extensions/second-string/tests \
  --sail-binary /path/to/native/release/sail --execution-mode local
python -m pytest examples/extensions/second-string/tests \
  --sail-binary /path/to/native/release/sail --execution-mode process-cluster
```

On macOS, Rust tests require the selected interpreter's library directory in
`DYLD_LIBRARY_PATH`, and `PYO3_PYTHON` should name that interpreter.
`tests/fixtures/oracle.json` records the compiled Scala reference and its source,
class and dependency fingerprints. `scripts/generate_oracle.py --help` gives the
fixture-generation command. The research and qualification report is kept in
[Grust](https://github.com/querygraph/grust/tree/work/second-string-extension-report/docs/reviews/second-string-extension-2026-10-06).
