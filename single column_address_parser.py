#!/usr/bin/env python3
"""
fulladdress_parser.py

Parse a combined address held in a single column (default "FullAddress") into
separate normalized columns and append them to the CSV output while preserving
all original columns.

Parsed columns appended (defaults):
    parsed_address1, parsed_address2, parsed_city, parsed_state, parsed_zip

Parsing is heuristic-based and uses the same local normalization rules used in
the standardizer so state names become two-letter codes (e.g., California -> CA)
and ZIPs are normalized to 5 or 5-4 format.

Usage:
    python fulladdress_parser.py input.csv output.csv
    python fulladdress_parser.py input.csv output.csv --column "FullAddress" --prefix parsed_
    cat input.csv | python fulladdress_parser.py - - --no-normalize
"""

from typing import Optional, Tuple, Dict
import re
import csv
import sys
import argparse

# --- reuse normalization helpers (compact version) ---
NON_ALNUM_RE = re.compile(r"[^\w\s#-]")
MULTISPACE_RE = re.compile(r"\s+")
POBOX_RE = re.compile(r"\bP\.?O\.?\s*BOX\b", re.IGNORECASE)

UNIT_DESIGNATORS = ["APT", "APARTMENT", "UNIT", "STE", "SUITE", "FL", "FLOOR", "RM", "ROOM", "#"]

DIR_MAP = {"NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
           "NORTHEAST": "NE", "NORTHWEST": "NW", "SOUTHEAST": "SE", "SOUTHWEST": "SW",
           "N.": "N", "S.": "S", "E.": "E", "W.": "W"}

SUFFIX_MAP = {
    "STREET": "ST", "AVENUE": "AVE", "ROAD": "RD", "DRIVE": "DR", "BOULEVARD": "BLVD",
    "COURT": "CT", "LANE": "LN", "TERRACE": "TER", "PLACE": "PL", "CIRCLE": "CIR",
}

STATE_MAP = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR", "CALIFORNIA": "CA",
    "NEW YORK": "NY", "TEXAS": "TX", "FLORIDA": "FL", "ILLINOIS": "IL", "WASHINGTON": "WA",
    # (include more states as needed)
}

def upper_and_cleanup(text: Optional[str]) -> str:
    if not text:
        return ""
    t = text.strip().upper()
    t = NON_ALNUM_RE.sub("", t)
    t = MULTISPACE_RE.sub(" ", t)
    return t

def standardize_state(state: str) -> str:
    if not state:
        return ""
    s = state.strip().upper()
    if len(s) == 2 and s.isalpha():
        return s
    s_clean = re.sub(r"[^\w\s]", "", s)
    s_clean = MULTISPACE_RE.sub(" ", s_clean).strip()
    if s_clean in STATE_MAP:
        return STATE_MAP[s_clean]
    s_alt = s_clean.replace(" STATE", "")
    if s_alt in STATE_MAP:
        return STATE_MAP[s_alt]
    return s

def standardize_zip(zip_code: str) -> str:
    if not zip_code:
        return ""
    z = re.sub(r"[^\d]", "", zip_code)
    if len(z) == 9:
        return f"{z[:5]}-{z[5:]}"
    if len(z) == 5:
        return z
    return z if z else zip_code.strip()

def replace_suffixes_and_dirs(s: str) -> str:
    if not s:
        return s
    for long_dir, short_dir in DIR_MAP.items():
        s = re.sub(r"\b" + re.escape(long_dir) + r"\b", short_dir, s)
    for long_suf, abb in SUFFIX_MAP.items():
        s = re.sub(r"\b" + re.escape(long_suf) + r"\b", abb, s)
    return MULTISPACE_RE.sub(" ", s).strip()

def extract_unit(address: str) -> Tuple[str, Optional[str]]:
    if not address:
        return "", None
    a = address.replace(",", " ")
    a = MULTISPACE_RE.sub(" ", a).strip()
    if POBOX_RE.search(a):
        return a, None
    trailing_unit_re = re.compile(
        r"\b(?P<designator>(" + "|".join(re.escape(x) for x in UNIT_DESIGNATORS) + r"))\.?\s*#?:?\s*(?P<value>[A-Z0-9-]+)$",
        re.IGNORECASE,
    )
    m = trailing_unit_re.search(a)
    if m:
        designator = m.group("designator").upper().replace("APARTMENT", "APT").replace("SUITE", "STE")
        if designator == "#":
            designator = "APT"
        value = m.group("value").upper()
        unit = f"{designator} {value}"
        addr_wo = a[: m.start()].strip()
        return addr_wo, unit
    mid_unit_re = re.compile(
        r"(?:,|\s)\s*(?P<designator>(" + "|".join(re.escape(x) for x in UNIT_DESIGNATORS) + r"))\.?\s*#?:?\s*(?P<value>[A-Z0-9-]+)\b",
        re.IGNORECASE,
    )
    m2 = mid_unit_re.search(a)
    if m2:
        designator = m2.group("designator").upper().replace("APARTMENT", "APT").replace("SUITE", "STE")
        if designator == "#":
            designator = "APT"
        value = m2.group("value").upper()
        unit = f"{designator} {value}"
        addr_wo = (a[: m2.start()] + a[m2.end() :]).strip()
        addr_wo = MULTISPACE_RE.sub(" ", addr_wo).strip()
        return addr_wo, unit
    return a, None

def standardize_address_components_local(
    address1: str,
    address2: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    zip_code: Optional[str] = None,
) -> Dict[str, str]:
    a1 = upper_and_cleanup(address1)
    a2 = upper_and_cleanup(address2) if address2 else ""
    if POBOX_RE.search(a1):
        a1 = POBOX_RE.sub("PO BOX", a1).replace(".", "")
    else:
        if not a2:
            a1, extracted_unit = extract_unit(a1)
            if extracted_unit:
                a2 = extracted_unit

    if a2:
        a2 = a2.replace("APARTMENT", "APT").replace("SUITE", "STE")
        a2 = MULTISPACE_RE.sub(" ", a2).strip()

    a1 = replace_suffixes_and_dirs(a1)
    if a2:
        a2 = replace_suffixes_and_dirs(a2)

    st = standardize_state(state or "")
    z = standardize_zip(zip_code or "")
    c = upper_and_cleanup(city)
    a1 = MULTISPACE_RE.sub(" ", a1).strip()
    a2 = MULTISPACE_RE.sub(" ", a2).strip()

    return {"address1": a1, "address2": a2, "city": c, "state": st, "zip": z}

# ---------- full-address parsing ----------
def parse_full_address(full: Optional[str]) -> Tuple[str, str, str, str, str]:
    """
    Heuristically parse a full address text into (address1, address2, city, state, zip)
    and return normalized components (CMS-like).
    """
    if not full:
        return "", "", "", "", ""
    s = full.strip()
    s = s.replace("\n", " ").replace("\r", " ")
    s = MULTISPACE_RE.sub(" ", s).strip()
    parts = [p.strip() for p in s.split(",") if p.strip()]

    street = ""
    unit = ""
    city = ""
    state = ""
    zipc = ""

    try:
        if len(parts) >= 3:
            street_part = ", ".join(parts[:-2])
            city_part = parts[-2]
            state_zip_part = parts[-1]
            m_zip = re.search(r"(\d{5}(?:-\d{4})?)\b", state_zip_part)
            if m_zip:
                zipc = m_zip.group(1)
                state_part = state_zip_part[:m_zip.start()].strip()
            else:
                state_part = state_zip_part.strip()
            street_candidate, unit_candidate = extract_unit(street_part)
            street = street_candidate
            unit = unit_candidate or ""
            city = city_part
            state = state_part
        elif len(parts) == 2:
            street_part = parts[0]
            right = parts[1]
            m_zip = re.search(r"(\d{5}(?:-\d{4})?)\b", right)
            if m_zip:
                zipc = m_zip.group(1)
                without_zip = right[:m_zip.start()].strip()
            else:
                without_zip = right
            tokens = without_zip.split()
            if tokens:
                state_candidate = tokens[-1]
                city_candidate = " ".join(tokens[:-1]).strip()
                state = state_candidate
                city = city_candidate
            else:
                city = without_zip
                state = ""
            street_candidate, unit_candidate = extract_unit(street_part)
            street = street_candidate
            unit = unit_candidate or ""
        else:
            m_zip = re.search(r"(\d{5}(?:-\d{4})?)\b", s)
            if m_zip:
                zipc = m_zip.group(1)
                before_zip = s[:m_zip.start()].strip()
            else:
                before_zip = s
            tokens = before_zip.split()
            idx_num = None
            for i, t in enumerate(tokens):
                if re.search(r"\d", t):
                    idx_num = i
                    break
            if idx_num is None:
                street = before_zip
            else:
                if len(tokens) >= 3 and (len(tokens[-1]) == 2 or tokens[-1].isalpha()):
                    state = tokens[-1]
                    street = " ".join(tokens[:len(tokens)-2])
                    city = ""
                else:
                    street = before_zip
    except Exception:
        return "", "", "", "", ""

    # Normalize using the same local normalizer so CMS format is enforced
    normalized = standardize_address_components_local(address1=street, address2=unit, city=city, state=state, zip_code=zipc)
    return (normalized.get("address1",""), normalized.get("address2",""), normalized.get("city",""), normalized.get("state",""), normalized.get("zip",""))

# ---------- CSV processor ----------
def process_csv(
    input_path: str,
    output_path: str,
    column_name: str = "FullAddress",
    prefix: str = "parsed_",
) -> None:
    with open(input_path, newline="", encoding="utf-8") as inf, open(output_path, "w", newline="", encoding="utf-8") as outf:
        reader = csv.DictReader(inf)
        input_fieldnames = reader.fieldnames or []

        # Find actual column name case-insensitively
        actual_col = None
        for fn in input_fieldnames:
            if fn and fn.strip().lower() == column_name.strip().lower():
                actual_col = fn
                break
        if not actual_col:
            print(f"Warning: Column {column_name!r} not found in input. No parsing will be done.", file=sys.stderr)

        new_cols = [f"{prefix}address1", f"{prefix}address2", f"{prefix}city", f"{prefix}state", f"{prefix}zip"]
        out_fieldnames = list(input_fieldnames) + new_cols
        writer = csv.DictWriter(outf, fieldnames=out_fieldnames)
        writer.writeheader()

        for row in reader:
            out_row = dict(row)
            if actual_col and row.get(actual_col):
                a1,a2,city,state,zipc = parse_full_address(row.get(actual_col))
            else:
                a1=a2=city=state=zipc=""
            out_row[new_cols[0]] = a1
            out_row[new_cols[1]] = a2
            out_row[new_cols[2]] = city
            out_row[new_cols[3]] = state
            out_row[new_cols[4]] = zipc
            writer.writerow(out_row)

# ---------- CLI ----------
def main(argv):
    p = argparse.ArgumentParser(description="Parse FullAddress into separate normalized columns.")
    p.add_argument("input", nargs="?", help="Input CSV file (use - for stdin)", default=None)
    p.add_argument("output", nargs="?", help="Output CSV file (default: stdout or parsed_output.csv)", default=None)
    p.add_argument("--column", default="FullAddress", help="Column name containing the full address (case-insensitive).")
    p.add_argument("--prefix", default="parsed_", help="Prefix to use for appended parsed columns.")
    args = p.parse_args(argv)

    if not args.input or args.input == "-":
        # Read stdin -> stdout
        reader = csv.DictReader(sys.stdin)
        input_fieldnames = reader.fieldnames or []
        # find actual column name
        actual_col = None
        for fn in input_fieldnames:
            if fn and fn.strip().lower() == args.column.strip().lower():
                actual_col = fn
                break
        new_cols = [f"{args.prefix}address1", f"{args.prefix}address2", f"{args.prefix}city", f"{args.prefix}state", f"{args.prefix}zip"]
        writer = csv.DictWriter(sys.stdout, fieldnames=list(input_fieldnames) + new_cols)
        writer.writeheader()
        for row in reader:
            out_row = dict(row)
            if actual_col and row.get(actual_col):
                a1,a2,city,state,zipc = parse_full_address(row.get(actual_col))
            else:
                a1=a2=city=state=zipc=""
            out_row[new_cols[0]] = a1
            out_row[new_cols[1]] = a2
            out_row[new_cols[2]] = city
            out_row[new_cols[3]] = state
            out_row[new_cols[4]] = zipc
            writer.writerow(out_row)
        return 0

    input_path = args.input
    output_path = args.output or "parsed_output.csv"
    process_csv(input_path, output_path, column_name=args.column, prefix=args.prefix)
    print(f"Wrote parsed addresses to {output_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))