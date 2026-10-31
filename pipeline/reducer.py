#!/usr/bin/env python3
"""Hadoop Streaming reducer for scalar or pipe-separated numeric values."""
import sys


def output(key, totals):
    if len(totals) == 1:
        value = str(int(totals[0]))
    else:
        value = "|".join(format(total, ".17g") for total in totals)
    print(f"{key}\t{value}")


current_key = None
totals = None
for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t", 1)
    values = [float(part) for part in value.split("|")]
    if key != current_key:
        if current_key is not None:
            output(current_key, totals)
        current_key = key
        totals = [0.0] * len(values)
    if len(values) != len(totals):
        raise ValueError(f"Inconsistent value width for key {key!r}")
    totals = [total + value for total, value in zip(totals, values)]
if current_key is not None:
    output(current_key, totals)
