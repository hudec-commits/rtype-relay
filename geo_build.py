"""
Makes geo.bin (country of an IP address, for relay.py) from the free DB-IP "IP to Country Lite" CSV
(https://db-ip.com/db/download/ip-to-country-lite, CC BY 4.0, monthly):

    python geo_build.py dbip-country-lite-2026-10.csv.gz      # -> geo.bin next to this script

geo.bin: "GEO1", the country codes (count, 2 bytes each), then IPv4 (count, start uint32[], code index
uint16[]) and IPv6 by its upper 64 bits (count, start uint64[], code index uint16[]), little endian.
Neighbouring ranges of the same country are merged; gaps become "ZZ" (unknown).
"""
import csv
import gzip
import ipaddress
import struct
import sys
from array import array
from pathlib import Path


def build(src, dst):
    codes, index = [], {}

    def code(c):
        c = (c or "ZZ").upper()[:2]
        if c not in index:
            index[c] = len(codes)
            codes.append(c)
        return index[c]

    code("ZZ")
    v4, v6 = [], []
    opener = gzip.open if str(src).endswith(".gz") else open
    with opener(src, "rt", encoding="ascii", newline="") as f:
        for row in csv.reader(f):
            if len(row) < 3:
                continue
            a, b = ipaddress.ip_address(row[0]), ipaddress.ip_address(row[1])
            if a.version == 4:
                v4.append((int(a), int(b), code(row[2])))
            else:
                v6.append((int(a) >> 64, int(b) >> 64, code(row[2])))

    def compact(ranges):
        ranges.sort()
        starts, ccs = [], []
        nxt = 0
        for s, e, c in ranges:
            if s > nxt:                      # a gap: unknown
                starts.append(nxt); ccs.append(0)
            if s < nxt:                      # IPv6 by 64 bits: overlapping /64 pieces, the first one wins
                s = nxt
                if s > e:
                    continue
            if not ccs or ccs[-1] != c:
                starts.append(s); ccs.append(c)
            nxt = e + 1
        return starts, ccs

    s4, c4 = compact(v4)
    s6, c6 = compact(v6)
    with open(dst, "wb") as out:
        out.write(b"GEO1")
        out.write(struct.pack("<I", len(codes)))
        out.write("".join(codes).encode("ascii"))
        out.write(struct.pack("<I", len(s4)))
        out.write(array("I", s4).tobytes())
        out.write(array("H", c4).tobytes())
        out.write(struct.pack("<I", len(s6)))
        out.write(array("Q", s6).tobytes())
        out.write(array("H", c6).tobytes())
    print(f"{dst}: {len(codes)} countries, {len(s4)} IPv4 and {len(s6)} IPv6 ranges")


if __name__ == "__main__":
    build(sys.argv[1], Path(__file__).with_name("geo.bin"))
