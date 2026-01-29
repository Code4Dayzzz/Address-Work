#!/usr/bin/env python3
"""
address_standardizer.py

Standardize CSV address components into CMS-like format, optionally calling
the USPS Verify API to get USPS-canonicalized fields.

Input: CSV with columns (by default) address1,address2,city,state,zip
Output: preserves all input columns and appends standardized columns prefixed
with "std_" (std_address1,std_address2,std_city,std_state,std_zip).

Usage:
    python address_standardizer.py input.csv output.csv --usps-userid YOUR_USERID
    python address_standardizer.py input.csv output.csv --no-usps
    cat input.csv | python address_standardizer.py - - --no-usps
"""

from typing import Optional, Tuple, Dict
import re
import csv
import sys
import os
import time
import argparse
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# Try optional requests
try:
    import requests  # type: ignore
    HAVE_REQUESTS = True
except Exception:
    HAVE_REQUESTS = False

# --- mappings & helpers (kept compact) ---
SUFFIX_MAP = {
    "STREET": "ST", "AVENUE": "AVE", "ROAD": "RD", "DRIVE": "DR", "BOULEVARD": "BLVD",
    "COURT": "CT", "LANE": "LN", "TERRACE": "TER", "PLACE": "PL", "CIRCLE": "CIR",
    # Add more as needed (you can reuse the larger mapping from your original script)
}
DIR_MAP = {"NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
           "NORTHEAST": "NE", "NORTHWEST": "NW", "SOUTHEAST": "SE", "SOUTHWEST": "SW",
           "N.": "N", "S.": "S", "E.": "E", "W.": "W"}
STATE_MAP = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR", "CALIFORNIA": "CA",
    "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE", "DISTRICT OF COLUMBIA": "DC",
    "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL",
    "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA",
    "MAINE": "ME", "MARYLAND": "MD", "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN",
    "MISSISSIPPI": "MS", "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY",
    "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK", "OREGON": "OR",
    "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT", "VIRGINIA": "VA",
    "WASHINGTON": "WA", "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY",
    "PUERTO RICO": "PR", "GUAM": "GU", "VIRGIN ISLANDS": "VI", "AMERICAN SAMOA": "AS",
}
UNIT_DESIGNATORS = ["APT", "APARTMENT", "UNIT", "STE", "SUITE", "FL", "FLOOR", "RM", "ROOM", "#"]

NON_ALNUM_RE = re.compile(r"[^\w\s#-]")
MULTISPACE_RE = re.compile(r"\s+")
POBOX_RE = re.compile(r"\bP\.?O\.?\s*BOX\b", re.IGNORECASE)

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

# ---------- USPS API integration ----------
USPS_API_URL = "https://secure.shippingapis.com/ShippingAPI.dll"

def build_usps_verify_xml(userid: str, street: str, secondary: str, city: str, state: str, zip5: str = "", zip4: str = "") -> str:
    def esc(s: str) -> str:
        if s is None:
            return ""
        return (s.replace("&", "&amp;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;")
                 .replace('"', "&quot;")
                 .replace("'", "&apos;"))
    xml = (
        f'<AddressValidateRequest USERID="{esc(userid)}">'
        f'<Revision>1</Revision>'
        f'<Address ID="0">'
        f'<Address1>{esc(secondary)}</Address1>'
        f'<Address2>{esc(street)}</Address2>'
        f'<City>{esc(city)}</City>'
        f'<State>{esc(state)}</State>'
        f'<Zip5>{esc(zip5)}</Zip5>'
        f'<Zip4>{esc(zip4)}</Zip4>'
        f'</Address>'
        f'</AddressValidateRequest>'
    )
    return xml

def call_usps_verify(userid: str, street: str, secondary: str, city: str, state: str, zip_code: str = "", timeout: int = 15):
    zip_clean = re.sub(r"[^\d]", "", zip_code or "")
    zip5 = zip_clean[:5] if len(zip_clean) >= 5 else ""
    zip4 = zip_clean[5:9] if len(zip_clean) >= 9 else ""
    xml = build_usps_verify_xml(userid, street or "", secondary or "", city or "", state or "", zip5, zip4)
    query = "API=Verify&XML=" + quote_plus(xml)
    try:
        if HAVE_REQUESTS:
            resp = requests.get(USPS_API_URL, params={"API": "Verify", "XML": xml}, timeout=timeout)
            content = resp.text
            if resp.status_code != 200:
                return False, {}, f"HTTP {resp.status_code}"
        else:
            url = USPS_API_URL + "?" + query
            req = Request(url, headers={"User-Agent": "addr-std-script/1.0"})
            with urlopen(req, timeout=timeout) as fh:
                content = fh.read().decode("utf-8")
    except HTTPError as he:
        return False, {}, f"HTTPError: {he.code} {he.reason}"
    except URLError as ue:
        return False, {}, f"URLError: {ue.reason}"
    except Exception as e:
        return False, {}, f"Request error: {e}"

    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        return False, {}, f"XML parse error: {e}. Raw: {content[:500]}"

    err = root.find(".//Error")
    if err is not None:
        desc = err.findtext("Description") or err.findtext("Number") or "USPS API error"
        return False, {}, f"USPS API error: {desc}"

    addr = root.find(".//Address")
    if addr is None:
        return False, {}, "No Address element in USPS response"
    usps_address2 = (addr.findtext("Address2") or "").strip()
    usps_address1 = (addr.findtext("Address1") or "").strip()
    usps_city = (addr.findtext("City") or "").strip()
    usps_state = (addr.findtext("State") or "").strip()
    usps_zip5 = (addr.findtext("Zip5") or "").strip()
    usps_zip4 = (addr.findtext("Zip4") or "").strip()
    combined_zip = usps_zip5
    if usps_zip5 and usps_zip4:
        combined_zip = f"{usps_zip5}-{usps_zip4}"
    standardized = {
        "address1": usps_address2,
        "address2": usps_address1,
        "city": usps_city,
        "state": usps_state,
        "zip": combined_zip,
    }
    return True, standardized, None

# ---------- CSV processing ----------
def process_csv(
    input_path: str,
    output_path: str,
    use_usps: bool,
    userid: Optional[str],
    throttle_ms: int = 200,
    columns: Optional[Tuple[str, ...]] = None,
) -> None:
    if columns is None:
        columns = ("address1", "address2", "city", "state", "zip")

    with open(input_path, newline="", encoding="utf-8") as inf, open(output_path, "w", newline="", encoding="utf-8") as outf:
        reader = csv.DictReader(inf)
        input_fieldnames = reader.fieldnames or []

        # create standardized field names avoiding collisions
        std_fields = []
        for c in columns:
            std_name = f"std_{c}"
            suffix = 1
            base = std_name
            while std_name in input_fieldnames:
                std_name = f"{base}_{suffix}"
                suffix += 1
            std_fields.append(std_name)

        writer = csv.DictWriter(outf, fieldnames=list(input_fieldnames) + std_fields)
        writer.writeheader()

        for i, row in enumerate(reader, start=1):
            raw_a1 = row.get(columns[0], "")
            raw_a2 = row.get(columns[1], "") if columns[1] in row else ""
            raw_city = row.get(columns[2], "")
            raw_state = row.get(columns[3], "")
            raw_zip = row.get(columns[4], "")

            local = standardize_address_components_local(raw_a1, raw_a2, raw_city, raw_state, raw_zip)

            if use_usps and userid:
                ok, usps_res, err = call_usps_verify(
                    userid,
                    street=local["address1"],
                    secondary=local["address2"],
                    city=local["city"],
                    state=local["state"],
                    zip_code=local["zip"],
                )
                if ok:
                    out_std = {
                        std_fields[0]: usps_res.get("address1") or local["address1"],
                        std_fields[1]: usps_res.get("address2") or local["address2"],
                        std_fields[2]: usps_res.get("city") or local["city"],
                        std_fields[3]: usps_res.get("state") or local["state"],
                        std_fields[4]: usps_res.get("zip") or local["zip"],
                    }
                else:
                    print(f"Row {i}: USPS failed: {err}", file=sys.stderr)
                    out_std = {
                        std_fields[0]: local["address1"],
                        std_fields[1]: local["address2"],
                        std_fields[2]: local["city"],
                        std_fields[3]: local["state"],
                        std_fields[4]: local["zip"],
                    }
                if throttle_ms > 0:
                    time.sleep(throttle_ms / 1000.0)
            else:
                out_std = {
                    std_fields[0]: local["address1"],
                    std_fields[1]: local["address2"],
                    std_fields[2]: local["city"],
                    std_fields[3]: local["state"],
                    std_fields[4]: local["zip"],
                }

            out_row = dict(row)
            for idx in range(len(columns)):
                out_row[std_fields[idx]] = out_std.get(std_fields[idx], "")
            writer.writerow(out_row)

# ---------- CLI ----------
def main(argv):
    p = argparse.ArgumentParser(description="Standardize address components (optionally call USPS).")
    p.add_argument("input", nargs="?", help="Input CSV file (use - for stdin)", default=None)
    p.add_argument("output", nargs="?", help="Output CSV file (default: stdout or standardized_output.csv)", default=None)
    p.add_argument("--columns", help="Comma-separated input column names in order: address1,address2,city,state,zip", default=None)
    p.add_argument("--usps-userid", help="USPS Web Tools USERID (or set USPS_USERID env var)", default=None)
    p.add_argument("--no-usps", dest="use_usps", action="store_false", help="Do not call USPS API.")
    p.add_argument("--throttle-ms", type=int, default=200, help="Milliseconds to sleep between USPS calls.")
    args = p.parse_args(argv)

    columns = tuple(c.strip() for c in args.columns.split(",")) if args.columns else None
    usps_userid = args.usps_userid or os.environ.get("USPS_USERID")
    use_usps = args.use_usps if "use_usps" in args else True

    if use_usps and not usps_userid:
        print("Warning: USPS requested but no USERID provided; falling back to local normalization.", file=sys.stderr)
        use_usps = False

    if not args.input or args.input == "-":
        reader = csv.DictReader(sys.stdin)
        input_fieldnames = reader.fieldnames or []
        if columns is None:
            columns = ("address1", "address2", "city", "state", "zip")
        # compute std fieldnames avoiding collisions
        std_fields = []
        for c in columns:
            std_name = f"std_{c}"
            suffix = 1
            base = std_name
            while std_name in input_fieldnames:
                std_name = f"{base}_{suffix}"
                suffix += 1
            std_fields.append(std_name)
        writer = csv.DictWriter(sys.stdout, fieldnames=list(input_fieldnames) + std_fields)
        writer.writeheader()
        for i, row in enumerate(reader, start=1):
            raw_a1 = row.get(columns[0], "")
            raw_a2 = row.get(columns[1], "") if columns[1] in row else ""
            raw_city = row.get(columns[2], "")
            raw_state = row.get(columns[3], "")
            raw_zip = row.get(columns[4], "")

            local = standardize_address_components_local(raw_a1, raw_a2, raw_city, raw_state, raw_zip)
            if use_usps and usps_userid:
                ok, usps_res, err = call_usps_verify(
                    usps_userid,
                    street=local["address1"],
                    secondary=local["address2"],
                    city=local["city"],
                    state=local["state"],
                    zip_code=local["zip"],
                )
                if ok:
                    out_std_vals = {
                        std_fields[0]: usps_res.get("address1") or local["address1"],
                        std_fields[1]: usps_res.get("address2") or local["address2"],
                        std_fields[2]: usps_res.get("city") or local["city"],
                        std_fields[3]: usps_res.get("state") or local["state"],
                        std_fields[4]: usps_res.get("zip") or local["zip"],
                    }
                else:
                    print(f"Row {i}: USPS failed: {err}", file=sys.stderr)
                    out_std_vals = {
                        std_fields[0]: local["address1"],
                        std_fields[1]: local["address2"],
                        std_fields[2]: local["city"],
                        std_fields[3]: local["state"],
                        std_fields[4]: local["zip"],
                    }
            else:
                out_std_vals = {
                    std_fields[0]: local["address1"],
                    std_fields[1]: local["address2"],
                    std_fields[2]: local["city"],
                    std_fields[3]: local["state"],
                    std_fields[4]: local["zip"],
                }
            out_row = dict(row)
            for idx in range(len(columns)):
                out_row[std_fields[idx]] = out_std_vals.get(std_fields[idx], "")
            writer.writerow(out_row)
        return 0

    input_path = args.input
    output_path = args.output or "standardized_output.csv"
    process_csv(input_path, output_path, use_usps=use_usps and bool(usps_userid), userid=usps_userid, throttle_ms=args.throttle_ms, columns=columns)
    print(f"Wrote standardized addresses to {output_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))