#!/usr/bin/env python3
"""Hadoop Streaming mapper for the raw aviation CSV files."""
import csv
import math
import sys

mode = sys.argv[1] if len(sys.argv) > 1 else "route"
SEP = "\x1f"


def number(value):
    value = value.strip().replace(",", "")
    result = float(value) if value else 0.0
    return result if math.isfinite(result) else 0.0


for row in csv.reader(sys.stdin):
    try:
        if mode == "carrier":
            if row[0] != "ScheduledDomestic" or row[1].startswith("Total"):
                continue
            pax = number(row[7])
            if pax <= 0:
                continue
            flights = number(row[4])
            plf_text = row[10].strip()
            plf = number(plf_text)
            plf_count = int(bool(plf_text) and plf_text.lower() not in {"nan", "na", "n/a", "null"})
            key = SEP.join((row[1], str(int(row[2])), str(int(row[3]))))
            values = (pax, flights, plf, plf_count)
            print(f"{key}\t{'|'.join(format(value, '.17g') for value in values)}")
        elif mode == "city":
            if row[0] == "Year":
                continue
            first, second = sorted((row[2], row[3]))
            key = SEP.join((str(int(row[0])), str(int(row[1])), first, second))
            values = (number(row[4]) + number(row[5]), number(row[6]) + number(row[7]))
            print(f"{key}\t{'|'.join(format(value, '.17g') for value in values)}")
        elif mode in ("route", "airport", "year"):
            if row[0] == "Year":
                continue
            pax = number(row[4]) + number(row[5])
            if mode == "route":
                print(f"{'-'.join(sorted((row[2], row[3])))}\t{pax}")
            elif mode == "airport":
                print(f"{row[2]}\t{pax}")
                print(f"{row[3]}\t{pax}")
            else:
                print(f"{row[0]}\t{pax}")
        else:
            if row[0] != "ScheduledDomestic" or row[1].startswith("Total"):
                continue
            pax = number(row[7])
            print(f"{row[1]}\t{pax}" if mode == "airline" else f"{row[1]}|{row[2]}\t{pax}")
    except (ValueError, IndexError):
        continue
