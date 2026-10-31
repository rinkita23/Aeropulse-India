"""Canonical airport names shared by the local and distributed pipelines."""
import re


ALIAS = {
    "COCHIN": "KOCHI",
    "DABOLIM": "GOA",
    "MOPA": "GOA",
    "TRIVANDRUM": "THIRUVANANTHAPURAM",
    "VISHAKHAPATNAM": "VISAKHAPATNAM",
    "ALLAHABAD": "PRAYAGRAJ",
    "HIRASAR": "RAJKOT",
    "CUDDAPAH": "KADAPA",
    "PONDICHERRY": "PUDUCHERRY",
    "BANGALORE": "BENGALURU",
    "BELAGAVI": "BELGAUM",
    "HUBBALLI": "HUBLI",
    "DEHRA DUN": "DEHRADUN",
    "TIRUCHIRAPALLY": "TIRUCHIRAPPALLI",
    "RAJKOT INTERNATIONAL AIRPORT": "RAJKOT",
    "AYODHYA INTERNATIONAL AIRPORT": "AYODHYA",
    "NASIK": "NASHIK",
    "MOPA, GOA": "GOA",
    "HINDON AIRPORT": "HINDON",
    "GHAZIABAD": "HINDON",
    "MYSORE": "MYSURU",
    "VIJAYWADA": "VIJAYAWADA",
}


def norm(name):
    name = re.sub(r"\s*\(.*\)", "", str(name).upper()).strip()
    return ALIAS.get(name, name)
