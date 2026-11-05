"""Render the README wordmark: "raftkv" in a 5x7 pixel font where every
pixel is a log entry. Entries are appended left to right (hollow, i.e.
uncommitted), and a commit index sweeps along behind them and fills them
in. Colours are the dashboard's term colours; each pair of letters is a new
term. SMIL animation, loops.

    python3 docs/media/make_wordmark.py
"""
import colorsys
import os

GLYPHS = {
    "r": [".....", ".....", "#.##.", "##..#", "#....", "#....", "#...."],
    "a": [".....", ".....", ".###.", "....#", ".####", "#...#", ".####"],
    "f": ["..##.", ".#..#", ".#...", "####.", ".#...", ".#...", ".#..."],
    "t": [".#...", ".#...", "####.", ".#...", ".#...", ".#..#", "..##."],
    "k": ["#....", "#....", "#..#.", "#.#..", "##...", "#.#..", "#..#."],
    "v": [".....", ".....", "#...#", "#...#", "#...#", ".#.#.", "..#.."],
}
WORD = "raftkv"
CELL, GAP, LETTER_GAP = 13, 3, 13
STEP = 0.045        # seconds between appended entries
COMMIT_LAG = 0.55   # commit index trails the log by this much
HOLD, FADE = 3.2, 0.5


def term_color(t):
    # same formula as dashboard/index.html: hsl((t*67+200) % 360, 65%, 52%)
    h = ((t * 67 + 200) % 360) / 360
    r, g, b = colorsys.hls_to_rgb(h, 0.52, 0.65)
    return "#%02x%02x%02x" % (int(r * 255), int(g * 255), int(b * 255))


def main():
    cells = []  # (x, y, term)
    x0 = 0
    for li, ch in enumerate(WORD):
        rows = GLYPHS[ch]
        term = li // 2 + 1
        for col in range(5):
            for row in range(7):
                if rows[row][col] == "#":
                    cells.append((x0 + col * (CELL + GAP), row * (CELL + GAP), term))
        x0 += 5 * (CELL + GAP) + LETTER_GAP
    width = x0 - LETTER_GAP - GAP
    height = 7 * (CELL + GAP) - GAP
    # order = log index: column by column, left to right
    cells.sort(key=lambda c: (c[0], c[1]))
    total = len(cells) * STEP + COMMIT_LAG + HOLD + FADE
    pad = 4
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width + 2*pad}" height="{height + 2*pad}" '
           f'viewBox="{-pad} {-pad} {width + 2*pad} {height + 2*pad}" role="img" aria-label="raftkv">',
           '<title>raftkv</title>',
           f'<rect width="0" height="0"><animate id="clk" attributeName="x" from="0" to="0" dur="{total:.2f}s" begin="0s;clk.end"/></rect>',
           '<g>',
           f'<animate attributeName="opacity" values="1;1;0" keyTimes="0;{(total - FADE) / total:.4f};1" dur="{total:.2f}s" begin="clk.begin"/>']
    for i, (x, y, term) in enumerate(cells):
        c = term_color(term)
        a = 0.3 + i * STEP
        k = a + COMMIT_LAG
        rest = total - a
        out.append(
            f'<rect x="{x}" y="{y}" width="{CELL}" height="{CELL}" rx="2.5" fill="{c}" fill-opacity="0" '
            f'stroke="{c}" stroke-width="1.6" opacity="0">'
            f'<set attributeName="opacity" to="1" begin="clk.begin+{a:.3f}s" dur="{rest:.3f}s"/>'
            f'<animate attributeName="fill-opacity" values="0;1" begin="clk.begin+{k:.3f}s" dur="0.18s" fill="freeze"/>'
            f'<set attributeName="fill-opacity" to="0" begin="clk.begin" dur="{k:.3f}s"/>'
            '</rect>')
    out.append('</g></svg>')
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "wordmark.svg"), "w") as f:
        f.write("\n".join(out))
    print(f"wrote wordmark.svg ({len(cells)} entries, {total:.1f}s loop)")


if __name__ == "__main__":
    main()
