"""Render the animated terminal recordings used in the README.

The sessions below are real output captured from raftkv (see the README for
how they were produced). Each line is a (kind, text) pair:
  "$"  a command, typed out character by character
  "#"  a comment, typed in a muted colour
  "o"  output, printed at once
  "!"  output, highlighted
Animation is SMIL, which GitHub renders inside <img>, and loops forever.

    python3 docs/media/make_terminals.py
"""
from html import escape

FONT = "ui-monospace, SFMono-Regular, Menlo, Consolas, 'Liberation Mono', monospace"
CW, LH, FS = 7.85, 19, 13  # char width, line height, font size
PAD_X, TOP = 20, 44
COLORS = {
    "bg": "#15181c", "bar": "#1d2126", "fg": "#d6dbe1", "muted": "#7d8794",
    "prompt": "#6fcf97", "hl": "#ff7b72", "title": "#8c95a1", "edge": "#2a3038",
}


def render(lines, title, width_chars=88, pause_end=4.0, type_speed=0.035):
    t = 0.6
    events = []  # (line_index, start, duration)
    for kind, text in lines:
        if kind in "$#":
            d = max(0.25, len(text) * type_speed)
            events.append((t, d))
            t += d + (0.35 if kind == "$" else 0.25)
        else:
            events.append((t, 0))
            t += 0.12
    total = t + pause_end
    w = int(PAD_X * 2 + width_chars * CW)
    h = int(TOP + len(lines) * LH + 18)
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" role="img" aria-label="{escape(title)}">',
           f'<title>{escape(title)}</title>',
           f'<rect width="{w}" height="{h}" rx="10" fill="{COLORS["bg"]}" stroke="{COLORS["edge"]}"/>',
           f'<path d="M0 10a10 10 0 0 1 10-10h{w-20}a10 10 0 0 1 10 10v18H0z" fill="{COLORS["bar"]}"/>',
           f'<text x="{PAD_X}" y="19" font-family="{FONT}" font-size="11.5" fill="{COLORS["title"]}">{escape(title)}</text>',
           # master clock: restarts itself, everything else hangs off its begin
           f'<rect width="0" height="0"><animate id="clk" attributeName="x" from="0" to="0" dur="{total:.2f}s" begin="0s;clk.end"/></rect>']
    for i, ((kind, text), (start, dur)) in enumerate(zip(lines, events)):
        y = TOP + i * LH
        vis = total - start
        color = {"$": COLORS["fg"], "#": COLORS["muted"], "o": COLORS["fg"], "!": COLORS["hl"]}[kind]
        body = escape(text)
        g = [f'<g opacity="0"><set attributeName="opacity" to="1" begin="clk.begin+{start:.2f}s" dur="{vis:.2f}s"/>']
        if kind == "$":
            line = f'<tspan fill="{COLORS["prompt"]}">$ </tspan>{body}'
            n = len(text) + 2
        elif kind == "#":
            line = f'# {body}'
            n = len(text) + 2
        else:
            line, n = body, len(text)
        if kind in "$#":
            cid = f"c{i}"
            full = n * CW + 2
            steps = max(2, n)
            vals = ";".join(f"{(k * full / steps):.1f}" for k in range(steps + 1))
            g.append(f'<clipPath id="{cid}"><rect x="{PAD_X}" y="{y-14}" height="{LH}" width="0">'
                     f'<animate attributeName="width" values="{vals}" calcMode="discrete" begin="clk.begin+{start:.2f}s" dur="{dur:.2f}s" fill="freeze"/></rect></clipPath>')
            g.append(f'<text x="{PAD_X}" y="{y}" font-family="{FONT}" font-size="{FS}" fill="{color}" xml:space="preserve" clip-path="url(#{cid})">{line}</text>')
            # cursor that rides along while typing, then blinks until the output appears
            cx = ";".join(f"{(PAD_X + k * full / steps):.1f}" for k in range(steps + 1))
            hold = 0.35 if kind == "$" else 0.25
            g.append(f'<rect y="{y-12}" width="7.5" height="15" fill="{COLORS["prompt"]}" opacity="0">'
                     f'<animate attributeName="x" values="{cx}" calcMode="discrete" begin="clk.begin+{start:.2f}s" dur="{dur:.2f}s" fill="freeze"/>'
                     f'<set attributeName="opacity" to="0.85" begin="clk.begin+{start:.2f}s" dur="{dur + hold:.2f}s"/></rect>')
        else:
            g.append(f'<text x="{PAD_X}" y="{y}" font-family="{FONT}" font-size="{FS}" fill="{color}" xml:space="preserve">{line}</text>')
        g.append('</g>')
        out.extend(g)
    out.append('</svg>')
    return "\n".join(out)


FAILOVER = [
    ("#", "three raftkv-server processes on :7001-7003"),
    ("$", "raftkv-cli put city lisbon"),
    ("$", "raftkv-cli append city ,porto"),
    ("$", "raftkv-cli get city"),
    ("o", "lisbon,porto"),
    ("$", "raftkv-cli status"),
    ("o", "ADDRESS            ID   ROLE            TERM  COMMIT    LAST   SNAP"),
    ("o", "127.0.0.1:7001     1    follower           1       3       3      0"),
    ("o", "127.0.0.1:7002     2    leader *           1       3       3      0"),
    ("o", "127.0.0.1:7003     3    follower           1       3       3      0"),
    ("#", "node 2 leads. pull the plug on it."),
    ("$", "kill -9 $(pgrep -f 'raftkv-server -id 2')"),
    ("$", "raftkv-cli get city"),
    ("o", "lisbon,porto"),
    ("#", "330 ms: one election, then the read went through"),
    ("$", "raftkv-cli put city lisbon,porto,faro"),
    ("$", "raftkv-cli get city"),
    ("o", "lisbon,porto,faro"),
    ("$", "raftkv-cli status"),
    ("o", "ADDRESS            ID   ROLE            TERM  COMMIT    LAST   SNAP"),
    ("o", "127.0.0.1:7001     1    leader *           2       5       5      0"),
    ("o", "127.0.0.1:7002     -    unreachable"),
    ("o", "127.0.0.1:7003     3    follower           2       5       5      0"),
]

CAUGHT = [
    ("$", "go test ./sim -run TestRandomized -seeds=400 -v"),
    ("o", "    400 seeds: 577357 ops, 4766 elections, 5585 crashes, 3604 partitions,"),
    ("o", "               20419 snapshots (10890 sent), +368/-407 servers, 133428 dropped,"),
    ("o", "               18347 duplicated msgs"),
    ("o", "--- PASS: TestRandomized (38.77s)"),
    ("#", "now plant a bug: let the leader answer reads without checking"),
    ("#", "that a majority still follows it"),
    ("$", "go test ./sim -run TestRandomized -seed-start=300024 -seeds=1 -v"),
    ("o", "    seed 300024: history is not linearizable"),
    ("o", "      client 4   [426, 585] append(\"k0\", \"<4.36>\")"),
    ("o", "      client 3   [428, 449] put(\"k0\", \"<3.41>\")"),
    ("o", "      client 0   [434, 435] get(\"k0\") -> \"<4.35><3.40><4.36><0.32>\""),
    ("!", "      client 0   [436, 437] get(\"k0\") -> \"<4.35><3.40>\""),
    ("#", "client 0 read the value, then read an older one. a deposed"),
    ("#", "leader on the wrong side of a partition answered the second get."),
    ("o", "--- FAIL: TestRandomized (0.02s)"),
]

if __name__ == "__main__":
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "failover.svg"), "w") as f:
        f.write(render(FAILOVER, "raftkv-cli · killing the leader"))
    with open(os.path.join(here, "caught.svg"), "w") as f:
        f.write(render(CAUGHT, "go test ./sim · the simulator catching a planted bug", width_chars=92))
    print("wrote failover.svg, caught.svg")
