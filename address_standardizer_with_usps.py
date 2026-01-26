#!/usr/bin/env python3
"""
address_standardizer_with_usps.py

Ready-to-run Python script to standardize U.S. mailing addresses into a consistent,
CMS-like format, with optional USPS Address Validation (Verify API) integration.

Features:
- Local normalization heuristics (uppercasing, suffix/directional/state/ZIP normalization).
- Optional USPS Verify API call to get USPS-canonicalized address fields (Address1,
  Address2, City, State, Zip5, Zip4).
- Safe fallback to local normalization when USPS is unavailable or returns an error.
- CSV input/output processing with configurable column names.
- CLI options for USPS USERID (or read from USPS_USERID env var), throttling, and test mode.
- Minimal external dependency: will use `requests` if available, otherwise uses stdlib urllib.

Notes:
- USPS Web Tools API requires a USERID. Obtain one from https://www.usps.com/business/web-tools-apis/
  and provide it via --usps-userid or the USPS_USERID environment variable.
- USPS AddressVerify expects Address1 (secondary) and Address2 (primary street). This script maps:
    - CSV "address1" -> USPS Address2 (street)
    - CSV "address2" -> USPS Address1 (secondary / unit)
- This script is intended for batch normalization + USPS canonicalization, not for real-time production
  without respecting USPS Web Tools terms and rate limits.

Usage:
    python address_standardizer_with_usps.py input.csv output.csv --usps-userid YOUR_USERID
    python address_standardizer_with_usps.py --test
    python address_standardizer_with_usps.py input.csv output.csv --no-usps

Author: Copilot-style assistant for Code4Dayzzz
Date: 2026-01-21
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

# Try to import requests if available (more robust). Not required.
try:
    import requests  # type: ignore
    HAVE_REQUESTS = True
except Exception:
    HAVE_REQUESTS = False

# ---------- Mappings and helpers (same heuristics as earlier) ----------

SUFFIX_MAP = {
    "ALLEE": "ALY", "ALLEY": "ALY", "ALLY": "ALY", "ANEX": "ANX", "ANNEX": "ANX",
    "ARCADE": "ARC", "AVENUE": "AVE", "AVEN": "AVE", "AVN": "AVE", "BAYOU": "BYU",
    "BEACH": "BCH", "BEND": "BND", "BLUFF": "BLF", "BOULEVARD": "BLVD", "BOUL": "BLVD",
    "BRANCH": "BR", "BRIDGE": "BRG", "BROOK": "BRK", "BURG": "BG", "BYPASS": "BYP",
    "CAMP": "CP", "CANYON": "CYN", "CAPE": "CPE", "CAUSEWAY": "CSWY", "CENTER": "CTR",
    "CIRCLE": "CIR", "CLIFF": "CLF", "CLUB": "CLB", "COMMON": "CMN", "CORNER": "COR",
    "COURSE": "CRSE", "COURT": "CT", "COVE": "CV", "CREEK": "CRK", "CRESCENT": "CRES",
    "CROSSING": "XING", "DALE": "DL", "DAM": "DM", "DIVIDE": "DV", "DRIVE": "DR",
    "ESTATE": "EST", "EXPRESSWAY": "EXPY", "EXTENSION": "EXT", "FALLS": "FLS", "FERRY": "FRY",
    "FIELD": "FLD", "FLAT": "FLT", "FORD": "FRD", "FOREST": "FRST", "FORGE": "FGR",
    "FORWARD": "FWD", "GARDEN": "GDN", "GATEWAY": "GTWY", "GLEN": "GLN", "GREEN": "GRN",
    "GROVE": "GRV", "HARBOR": "HBR", "HAVEN": "HVN", "HEIGHTS": "HTS", "HIGHWAY": "HWY",
    "HILL": "HL", "HOLLOW": "HOLW", "INLET": "INLT", "ISLAND": "IS", "ISLE": "ISLE",
    "JUNCTION": "JCT", "KEY": "KY", "KNOLL": "KNL", "LAKE": "LK", "LANDING": "LNDG",
    "LANE": "LN", "LIGHT": "LGT", "LOAF": "LF", "LOCK": "LCK", "LODGE": "LDG",
    "LOOP": "LOOP", "MALL": "MALL", "MANOR": "MNR", "MEADOW": "MDW", "MILL": "ML",
    "MOUNT": "MT", "MOUNTAIN": "MTN", "NORTH": "N", "PARK": "PARK", "PARKWAY": "PKWY",
    "PASS": "PASS", "PASSAGE": "PSGE", "PATH": "PATH", "PIKE": "PIKE", "PINE": "PNE",
    "PLACE": "PL", "PLAZA": "PLZ", "POINT": "PT", "PORT": "PRT", "PRAIRIE": "PR",
    "RADIAL": "RADL", "RANCH": "RNCH", "RAPIDS": "RPD", "REST": "RST", "RIDGE": "RDG",
    "RIVER": "RIV", "ROAD": "RD", "ROW": "ROW", "RUN": "RUN", "SHORE": "SHR",
    "SPRING": "SPG", "SQUARE": "SQ", "STATION": "STA", "STRAVENUE": "STRA", "STREAM": "STRM",
    "STREET": "ST", "SUMMIT": "SMT", "TERRACE": "TER", "TRACE": "TRCE",
    "TRAIL": "TRL", "TUNNEL": "TUNL", "TURNPIKE": "TPKE", "UNION": "UN",
    "VALLEY": "VLY", "VIEW": "VW", "VILLAGE": "VLG", "VILLE": "VL", "WALK": "WALK",
    "WALL": "WALL", "WAY": "WAY", "WELL": "WL",
}

DIR_MAP = {
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
    "NORTHEAST": "NE", "NORTHWEST": "NW", "SOUTHEAST": "SE", "SOUTHWEST": "SW",
    "N.": "N", "S.": "S", "E.": "E", "W.": "W",
}

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

UNIT_DESIGNATORS = [
    "APT", "APARTMENT", "UNIT", "STE", "SUITE", "FL", "FLOOR", "RM", "ROOM", "#"
]

NON_ALNUM_RE = re.compile(r"[^\w\s#-]")
MULTISPACE_RE = re.compile(r"\s+")
POBOX_RE = re.compile(r"\bP\.?O\.?\s*BOX\b", re.IGNORECASE)

# ---------- Local normalization helpers ----------

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
        pattern = r"\b" + re.escape(long_dir) + r"\b"
        s = re.sub(pattern, short_dir, s)
    for long_suf, abb in SUFFIX_MAP.items():
        pattern = r"\b" + re.escape(long_suf) + r"\b"
        s = re.sub(pattern, abb, s)
    s = MULTISPACE_RE.sub(" ", s).strip()
    return s

def normalize_po_box(address: str) -> str:
    if not address:
        return address
    s = POBOX_RE.sub("PO BOX", address)
    s = s.replace(".", "")
    s = MULTISPACE_RE.sub(" ", s).strip()
    return s

def extract_unit(address: str) -> Tuple[str, Optional[str]]:
    if not address:
        return "", None
    a = address
    a = a.replace(",", " ")
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
        a1 = normalize_po_box(a1)
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
# The API key/USERID must be provided by the user.

def build_usps_verify_xml(userid: str, street: str, secondary: str, city: str, state: str, zip5: str = "", zip4: str = "") -> str:
    """
    Build USPS AddressValidateRequest XML (USERID must be valid).
    Mapping:
      - USPS Address1 = secondary (APT, etc.)
      - USPS Address2 = street (primary)
    """
    # Escape for XML. ET or manual replace is fine for simple fields.
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

def call_usps_verify(userid: str, street: str, secondary: str, city: str, state: str, zip_code: str = "", timeout: int = 15) -> Tuple[bool, Dict[str, str], Optional[str]]:
    """
    Call USPS Verify API. Returns (success, standardized_fields, error_message)
    standardized_fields keys: address1 (street), address2 (secondary/unit), city, state, zip (ZIP5 or ZIP5-ZIP4)
    On failure, success=False and error_message contains the USPS error description or HTTP error.
    """
    # Prepare zip5/zip4
    zip_clean = re.sub(r"[^\d]", "", zip_code or "")
    zip5 = zip_clean[:5] if len(zip_clean) >= 5 else ""
    zip4 = zip_clean[5:9] if len(zip_clean) >= 9 else ""

    xml = build_usps_verify_xml(userid, street or "", secondary or "", city or "", state or "", zip5, zip4)
    params = {"API": "Verify", "XML": xml}
    # Encode as query param (GET)
    query = "API=Verify&XML=" + quote_plus(xml)

    try:
        if HAVE_REQUESTS:
            resp = requests.get(USPS_API_URL, params={"API": "Verify", "XML": xml}, timeout=timeout)
            content = resp.text
            status = resp.status_code
            if status != 200:
                return False, {}, f"HTTP {status}"
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

    # Parse XML response
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        return False, {}, f"XML parse error: {e}. Raw response: {content[:500]}"

    # Check for error node
    err = root.find(".//Error")
    if err is not None:
        # USPS error structure: <Error><Number>...</Number><Description>...</Description></Error>
        desc = err.findtext("Description") or (err.findtext("Number") or "USPS API error")
        return False, {}, f"USPS API error: {desc}"

    addr = root.find(".//Address")
    if addr is None:
        return False, {}, "No Address element in USPS response"

    # Extract fields (Address2 is street, Address1 is secondary/unit)
    usps_address2 = (addr.findtext("Address2") or "").strip()
    usps_address1 = (addr.findtext("Address1") or "").strip()
    usps_city = (addr.findtext("City") or "").strip()
    usps_state = (addr.findtext("State") or "").strip()
    usps_zip5 = (addr.findtext("Zip5") or "").strip()
    usps_zip4 = (addr.findtext("Zip4") or "").strip()

    combined_zip = usps_zip5
    if usps_zip5 and usps_zip4:
        combined_zip = f"{usps_zip5}-{usps_zip4}"

    # The USPS returns uppercase canonical fields typically already standardized.
    # Map back to our output: address1=street (Address2), address2=secondary (Address1)
    standardized = {
        "address1": usps_address2,
        "address2": usps_address1,
        "city": usps_city,
        "state": usps_state,
        "zip": combined_zip,
    }
    return True, standardized, None

# ---------- CSV Processing with optional USPS ----------

def process_csv_with_usps(
    input_path: str,
    output_path: str,
    use_usps: bool,
    userid: Optional[str],
    throttle_ms: int = 200,
    columns: Optional[Tuple[str, ...]] = None,
) -> None:
    """
    Process CSV, attempt USPS verification for each row if enabled and USERID provided.
    columns tuple: (address1,address2,city,state,zip). If None, defaults to these names.
    """
    if columns is None:
        columns = ("address1", "address2", "city", "state", "zip")

    with open(input_path, newline="", encoding="utf-8") as inf, open(output_path, "w", newline="", encoding="utf-8") as outf:
        reader = csv.DictReader(inf)
        fieldnames = list(columns)
        writer = csv.DictWriter(outf, fieldnames=fieldnames)
        writer.writeheader()

        for i, row in enumerate(reader, start=1):
            raw_a1 = row.get(columns[0], "")
            raw_a2 = row.get(columns[1], "") if columns[1] in row else ""
            raw_city = row.get(columns[2], "")
            raw_state = row.get(columns[3], "")
            raw_zip = row.get(columns[4], "")

            # First apply local normalization
            local_norm = standardize_address_components_local(raw_a1, raw_a2, raw_city, raw_state, raw_zip)

            if use_usps and userid:
                # USPS expects address2=street, address1=secondary
                usps_ok, usps_result, usps_err = call_usps_verify(
                    userid,
                    street=local_norm["address1"],      # our street -> USPS Address2
                    secondary=local_norm["address2"],   # our address2 -> USPS Address1
                    city=local_norm["city"],
                    state=local_norm["state"],
                    zip_code=local_norm["zip"] or "",
                )
                if usps_ok:
                    # Merge: prefer USPS fields when present; fall back to local if empty
                    out = {
                        "address1": usps_result.get("address1") or local_norm["address1"],
                        "address2": usps_result.get("address2") or local_norm["address2"],
                        "city": usps_result.get("city") or local_norm["city"],
                        "state": usps_result.get("state") or local_norm["state"],
                        "zip": usps_result.get("zip") or local_norm["zip"],
                    }
                else:
                    # On USPS failure, keep local_norm and optionally log error to stderr
                    print(f"Row {i}: USPS failed: {usps_err}", file=sys.stderr)
                    out = local_norm
                # Throttle between API calls to avoid rate limits
                if throttle_ms > 0:
                    time.sleep(throttle_ms / 1000.0)
            else:
                out = local_norm

            writer.writerow({
                columns[0]: out["address1"],
                columns[1]: out["address2"],
                columns[2]: out["city"],
                columns[3]: out["state"],
                columns[4]: out["zip"],
            })

# ---------- Small test harness demonstrating USPS integration ----------

TEST_CASES = [
    ("123 Main Street", "", "Anytown", "New York", "12345"),
    ("456 Elm St Apt 4B", "", "Smallville", "Illinois", "60606-1234"),
    ("PO Box 789", "", "Post City", "California", "90210"),
    ("100 North Broadway Suite 200", None, "Bigcity", "tx", "73301"),
    ("12-34 W. Maple Avenue, #5", "", "Sample", "Florida", "33101"),
    ("742 Evergreen Terrace", "", "Springfield", "illinois", "62704"),
    ("1600 Pennsylvania Ave NW", "", "Washington", "district of columbia", "20500"),
    ("500 S HIGHWAY 101", "UNIT 3", "Coastal", "oregon", "97101-1234"),
]

def run_tests(usps_userid: Optional[str] = None, use_usps: bool = False):
    print("Running quick tests...\n")
    print(f"USPS integration enabled: {use_usps}, USERID provided: {'yes' if usps_userid else 'no'}\n")
    for i, (a1, a2, city, state, zipc) in enumerate(TEST_CASES, 1):
        local = standardize_address_components_local(a1, a2, city, state, zipc)
        print(f"Case {i}: Input: {a1!r}, {a2!r}, {city!r}, {state!r}, {zipc!r}")
        print("  Local normalized:", local)
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
                print("  USPS result:", usps_res)
            else:
                print("  USPS error:", err)
        print()

# ---------- CLI ----------

def main(argv):
    p = argparse.ArgumentParser(description="Standardize US mailing addresses (with optional USPS Verify API).")
    p.add_argument("input", nargs="?", help="Input CSV file (default: stdin)", default=None)
    p.add_argument("output", nargs="?", help="Output CSV file (default: stdout)", default=None)
    p.add_argument("--columns", help="Comma-separated input column names in order: address1,address2,city,state,zip", default=None)
    p.add_argument("--usps-userid", help="USPS Web Tools USERID (can also be set via USPS_USERID env var).", default=None)
    p.add_argument("--no-usps", dest="use_usps", action="store_false", help="Do not call the USPS API (use local normalization only).")
    p.add_argument("--throttle-ms", type=int, default=200, help="Milliseconds to sleep between USPS calls (default: 200).")
    p.add_argument("--test", action="store_true", help="Run built-in tests and exit")
    args = p.parse_args(argv)

    columns = tuple(c.strip() for c in args.columns.split(",")) if args.columns else None
    usps_userid = args.usps_userid or os.environ.get("USPS_USERID")
    use_usps = args.use_usps if "use_usps" in args else True

    if args.test:
        run_tests(usps_userid, use_usps=use_usps and bool(usps_userid))
        return 0

    if use_usps and not usps_userid:
        print("Warning: USPS integration requested but no USERID provided (use --usps-userid or set USPS_USERID). Falling back to local normalization.", file=sys.stderr)
        use_usps = False

    if not args.input or args.input == "-":
        # Read from stdin, write to stdout
        reader = csv.DictReader(sys.stdin)
        fieldnames = list(columns) if columns else ["address1", "address2", "city", "state", "zip"]
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        for i, row in enumerate(reader, start=1):
            raw_a1 = row.get(fieldnames[0], "")
            raw_a2 = row.get(fieldnames[1], "") if fieldnames[1] in row else ""
            raw_city = row.get(fieldnames[2], "")
            raw_state = row.get(fieldnames[3], "")
            raw_zip = row.get(fieldnames[4], "")
            local_norm = standardize_address_components_local(raw_a1, raw_a2, raw_city, raw_state, raw_zip)
            if use_usps and usps_userid:
                ok, usps_res, err = call_usps_verify(
                    usps_userid,
                    street=local_norm["address1"],
                    secondary=local_norm["address2"],
                    city=local_norm["city"],
                    state=local_norm["state"],
                    zip_code=local_norm["zip"],
                )
                if ok:
                    out = {
                        fieldnames[0]: usps_res.get("address1") or local_norm["address1"],
                        fieldnames[1]: usps_res.get("address2") or local_norm["address2"],
                        fieldnames[2]: usps_res.get("city") or local_norm["city"],
                        fieldnames[3]: usps_res.get("state") or local_norm["state"],
                        fieldnames[4]: usps_res.get("zip") or local_norm["zip"],
                    }
                else:
                    print(f"Row {i}: USPS failed: {err}", file=sys.stderr)
                    out = {
                        fieldnames[0]: local_norm["address1"],
                        fieldnames[1]: local_norm["address2"],
                        fieldnames[2]: local_norm["city"],
                        fieldnames[3]: local_norm["state"],
                        fieldnames[4]: local_norm["zip"],
                    }
                if args.throttle_ms > 0:
                    time.sleep(args.throttle_ms / 1000.0)
            else:
                out = {
                    fieldnames[0]: local_norm["address1"],
                    fieldnames[1]: local_norm["address2"],
                    fieldnames[2]: local_norm["city"],
                    fieldnames[3]: local_norm["state"],
                    fieldnames[4]: local_norm["zip"],
                }
            writer.writerow(out)
        return 0

    # Files provided
    input_path = args.input
    output_path = args.output or "standardized_output.csv"
    process_csv_with_usps(input_path, output_path, use_usps=use_usps and bool(usps_userid), userid=usps_userid, throttle_ms=args.throttle_ms, columns=columns)
    print(f"Wrote standardized addresses to {output_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))