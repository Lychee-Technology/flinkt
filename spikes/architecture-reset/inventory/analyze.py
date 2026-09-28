"""Disposable spike: classify the stream API surface dumped by Inventory.java.

For DataStream, SingleOutputStreamOperator and KeyedStream this derives, per Flink line, what a
subtype façade would have to do with every public instance method (override covariantly, add a
reified overload, or leave it as a silent exit) and what a composition view would have to offer.
"""
import collections
import csv
import re
import sys

LINES = ["f120", "f22", "f23"]
PKG = "org.apache.flink.streaming.api.datastream."
FACADE = ["DataStream", "SingleOutputStreamOperator", "KeyedStream"]
STREAM_FAMILY = {"DataStream", "SingleOutputStreamOperator", "KeyedStream", "DataStreamSource",
                 "SideOutputDataStream", "IterativeStream", "CachedDataStream"}


def load(line):
    rows = []
    with open(f"inv-{line}.tsv") as f:
        for r in csv.reader(f, delimiter="\t"):
            r += [""] * (9 - len(r))
            rows.append(dict(line=r[0], kind=r[1], cls=r[2].replace(PKG, ""), name=r[3], mods=r[4],
                             ann=r[5], ret=r[6], params=r[7], decl=r[8]))
    return rows


def ret_class(ret):
    t = ret.split(" ", 1)[1] if ret.startswith("<") else ret.strip()
    t = t.strip()
    base = re.sub(r"<.*", "", t).split(".")[-1].replace("$", ".")
    return t, base


def method_tparams(ret):
    m = re.match(r"<([^>]*)>", ret.strip())
    return m.group(1).split(",") if m else []


def classify(row, cls):
    full, base = ret_class(row["ret"])
    tps = method_tparams(row["ret"])
    if base in STREAM_FAMILY:
        kind = "stream"
    elif base == "DataStreamSink":
        kind = "sink"
    elif full.startswith("org.apache.flink.streaming.api.datastream") or base in (
            "ConnectedStreams", "BroadcastConnectedStream", "WindowedStream", "AllWindowedStream",
            "JoinedStreams", "CoGroupedStreams", "BroadcastStream", "PartitionWindowedStream",
            "KeyedStream.IntervalJoin", "QueryableStateStream"):
        kind = "builder"
    else:
        kind = "other"
    # Does the method introduce a new element type? (a method type parameter appears in the returned
    # stream's type arguments)
    args = re.sub(r"^[^<]*", "", full)
    introduces = kind == "stream" and any(re.search(rf"\b{re.escape(tp.strip())}\b", args) for tp in tps)
    return kind, base, introduces


def main():
    data = {l: load(l) for l in LINES}
    out = []
    p = out.append
    for cls in FACADE:
        p(f"\n## {cls}")
        per_line = {}
        for l in LINES:
            ms = [r for r in data[l] if r["kind"] == "METHOD" and r["cls"] == cls and "static" not in r["mods"]
                  and "protected" not in r["mods"] and r["decl"] != "Object"]
            per_line[l] = ms
            counts = collections.Counter()
            for r in ms:
                kind, base, intro = classify(r, cls)
                counts[kind] += 1
                if intro:
                    counts["stream:introduces-type"] += 1
                if "final" in r["mods"]:
                    counts["final"] += 1
                if "@Deprecated" in r["ann"]:
                    counts["deprecated"] += 1
            p(f"- {l}: {len(ms)} public instance methods; " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
        # final methods
        for l in LINES:
            finals = sorted({r["name"] + r["params"] for r in per_line[l] if "final" in r["mods"]})
            p(f"- {l} final: {finals}")
        # signature diffs between lines
        sigs = {l: {r["name"] + r["params"]: r for r in per_line[l]} for l in LINES}
        for a, b in [("f120", "f22"), ("f22", "f23")]:
            only_a = sorted(set(sigs[a]) - set(sigs[b]))
            only_b = sorted(set(sigs[b]) - set(sigs[a]))
            p(f"- only in {a} vs {b}: {len(only_a)}; only in {b}: {len(only_b)}")
            for s in only_a:
                p(f"    - {a}: {s} -> {sigs[a][s]['ret']}")
            for s in only_b:
                p(f"    + {b}: {s} -> {sigs[b][s]['ret']}")
    # Detailed table for f23 and f120: every stream/builder-returning method on the façade classes
    for l in ["f23", "f120"]:
        p(f"\n## Stream-returning and builder-returning methods, {l}")
        p("| class | method(params) | returns | final | new element type | TypeInformation overload | decl |")
        p("|---|---|---|---|---|---|---|")
        for cls in FACADE:
            ms = [r for r in data[l] if r["kind"] == "METHOD" and r["cls"] == cls and "static" not in r["mods"]
                  and "protected" not in r["mods"]]
            names_with_ti = {r["name"] for r in ms if "TypeInformation" in r["params"]}
            for r in sorted(ms, key=lambda r: (r["name"], r["params"])):
                kind, base, intro = classify(r, cls)
                if kind not in ("stream", "builder"):
                    continue
                if r["decl"] != cls and cls != "DataStream" and not (cls == "KeyedStream" and r["decl"] == "KeyedStream"):
                    # inherited: listed under the declaring class already
                    continue
                params = re.sub(r"org\.apache\.flink\.[a-z.]*\.", "", r["params"])
                p(f"| {cls} | {r['name']}{params} | {base} | {'final' if 'final' in r['mods'] else ''} | "
                  f"{'yes' if intro else ''} | {'yes' if (intro and r['name'] in names_with_ti) else ('no' if intro else '')} | {r['decl']} |")
    # Fields: object-local state
    p("\n## Instance fields (object-local state)")
    for cls in FACADE + ["DataStreamSource", "SideOutputDataStream"]:
        for l in LINES:
            fs = [r for r in data[l] if r["kind"] == "FIELD" and r["cls"] == cls]
            p(f"- {cls} {l}: " + ", ".join(f"{r['decl']}.{r['name']}({r['mods']})" for r in fs))
    # Constructors
    p("\n## Constructors")
    for cls in FACADE + ["DataStreamSource", "SideOutputDataStream"]:
        for l in LINES:
            cs = [r for r in data[l] if r["kind"] == "CTOR" and r["cls"] == cls]
            p(f"- {cls} {l}: " + "; ".join(f"[{r['mods']}{' ' + r['ann'] if r['ann'] else ''}] {re.sub(r'org[.]apache[.]flink[.][a-z.]*[.]', '', r['params'])}" for r in cs))
    print("\n".join(out))


main()
