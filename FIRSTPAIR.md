slug: sail-declarative-pipelines
shelf: querygraph
default_edition: full

# FirstPair Library Contract

This file is read by the FirstPair publisher; keep the key-value header above simple and
unbulleted.

## Ownership

This source repository owns the *Sail Declarative Pipelines* technical report: the manuscript
`docs/books/sail-declarative-pipelines/sail-declarative-pipelines.md`, its metadata, cover,
styles, `book.build.json`, and the built artifacts in `docs/books/sail-declarative-pipelines/dist/`.
The runnable demo the report describes lives in `examples/declarative-pipelines/`.

The FirstPair repository at `~/src/firstpair` owns the unified builder, the public catalog,
hosted readers, Blob uploads and deployment. Follow `~/src/firstpair/AGENTS.md`.

## Build

Commit the source first: the builder stamps the Git HEAD into the edition.

```sh
"$HOME/src/firstpair/publishing/scripts/build-library-book.sh" --repo-root "$(git rev-parse --show-toplevel)"
```

The cover is rendered from `docs/books/sail-declarative-pipelines/cover/cover.typ`:

```sh
cd docs/books/sail-declarative-pipelines
typst compile --ppi 200 cover/cover.typ cover/sail-declarative-pipelines-cover.png
```

## Publish

Publishing to firstpair.org is a separate, outward-facing step that needs the owner's explicit
go-ahead. Inspect the plan first:

```sh
cd "$HOME/src/firstpair"
npm run library:publish -- /path/to/this/repo --dry-run --no-build --no-smoke --no-deploy --no-icloud
```
