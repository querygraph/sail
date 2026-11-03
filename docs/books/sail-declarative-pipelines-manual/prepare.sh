#!/usr/bin/env bash
# Concatenates the manual's chapters, in file order, into one manuscript.
# Both lanes' chapters live in chapters/; the number prefix orders them.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$here/build"
out="$here/build/sail-declarative-pipelines-manual.md"
: > "$out"
for chapter in "$here"/chapters/*.md; do
    cat "$chapter" >> "$out"
    printf '\n\n' >> "$out"
done
echo "manuscript: $out ($(wc -w < "$out" | tr -d ' ') words, $(ls "$here"/chapters/*.md | wc -l | tr -d ' ') chapters)"
