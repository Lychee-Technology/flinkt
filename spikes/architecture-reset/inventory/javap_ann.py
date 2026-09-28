import subprocess, re, sys
cp = sys.argv[1]
classes = sys.argv[2:]
for c in classes:
    out = subprocess.run(["javap", "-v", "-cp", cp, c], capture_output=True, text=True).stdout
    # split into member blocks; each member starts with an indented declaration line ending in ';'
    blocks = re.split(r"\n(?=  (?:public|protected|private|static|final|abstract|[a-zA-Z<].*\(.*\).*;))", out)
    for b in blocks:
        first = b.strip().split("\n")[0]
        if "(" not in first or not first.endswith(";"):
            continue
        m = re.search(r"RuntimeInvisibleAnnotations:(.*?)(?:\n    [A-Z]|\Z)", b, re.S)
        anns = re.findall(r"org\.apache\.flink\.annotation\.(\w+)", m.group(1)) if m else []
        dep = "Deprecated: true" in b or "java/lang/Deprecated" in b
        if anns or dep:
            print(f"{c.split('.')[-1]}\t{re.sub(r'org[.]apache[.]flink[.][a-z.]*[.]','',first)}\t{' '.join('@'+a for a in anns)}{' @Deprecated' if dep else ''}")
