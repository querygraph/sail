// Sail Declarative Pipelines cover: a dataflow graph, sources on the left, datasets on the right.
#set page(width: 8.5in, height: 11in, margin: 0pt, fill: rgb("#0e1b24"))
#set text(font: "Libertinus Serif", fill: rgb("#e9eef0"))

#let ink = rgb("#e9eef0")
#let teal = rgb("#4fb3bf")
#let sand = rgb("#d9b26f")
#let coral = rgb("#d9776a")
#let sage = rgb("#9cbf8a")

#let edge(a, b, c) = place(top + left, line(start: a, end: b, stroke: 1.6pt + c.transparentize(30%)))
#let node(p, w, label, c) = place(top + left, dx: p.at(0), dy: p.at(1),
  rect(width: w, height: 0.42in, radius: 4pt, fill: c.transparentize(82%), stroke: 1.2pt + c,
    align(center + horizon, text(size: 10.5pt, fill: ink, font: "DejaVu Sans Mono", label))))

// sources
#let s1 = (0.8in, 1.2in)
#let s2 = (0.8in, 2.3in)
#let s3 = (0.8in, 3.4in)
#let s4 = (0.8in, 4.5in)
// flows and datasets
#let d1 = (3.35in, 1.2in)
#let d2 = (3.35in, 2.85in)
#let d3 = (3.35in, 4.5in)
#let o1 = (5.9in, 1.75in)
#let o2 = (5.9in, 3.4in)
#let o3 = (5.9in, 4.5in)

#let east(p, w) = (p.at(0) + w, p.at(1) + 0.21in)
#let west(p) = (p.at(0), p.at(1) + 0.21in)

#edge(east(s1, 1.7in), west(d1), teal)
#edge(east(s2, 1.7in), west(d2), teal)
#edge(east(s3, 1.7in), west(d2), teal)
#edge(east(s4, 1.7in), west(d3), coral)
#edge(east(d1, 1.9in), west(o1), sand)
#edge(east(d2, 1.9in), west(o1), sand)
#edge(east(d2, 1.9in), west(o2), sand)
#edge(east(d3, 1.9in), west(o3), coral)

#node(s1, 1.7in, "orders_raw", teal)
#node(s2, 1.7in, "json files", teal)
#node(s3, 1.7in, "delta table", teal)
#node(s4, 1.7in, "iggy topic", coral)
#node(d1, 1.9in, "STREAMING TABLE", sage)
#node(d2, 1.9in, "append flows", sage)
#node(d3, 1.9in, "AUTO CDC", coral)
#node(o1, 1.8in, "MATERIALIZED", sand)
#node(o2, 1.8in, "VIEW", sand)
#node(o3, 1.8in, "SCD 1", coral)

#place(top + left, dx: 0.8in, dy: 5.55in, line(length: 6.9in, stroke: 0.6pt + ink.transparentize(70%)))

#place(top + left, dx: 0.8in, dy: 5.95in, block(width: 6.9in)[
  #set par(leading: 0.35em, spacing: 0pt)
  #text(size: 14pt, tracking: 0.26em, fill: sand)[FIRST PAIR PRESS]
  #v(0.35in)
  #text(size: 54pt, weight: "medium")[Sail Declarative \ Pipelines]
  #v(0.35in)
  #text(size: 19pt, style: "italic", fill: rgb("#c9d6da"))[Spark 4.2.1 Declarative Pipelines on Sail]
  #v(0.16in)
  #text(size: 15pt, fill: rgb("#c9d6da"))[Technical Report · Milestone 1]
  #v(0.45in)
  #text(size: 15pt)[Alexy Khrabrov ∈ LakeSail Team]
])
