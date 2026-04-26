"""
map_stations.py
Parses OASIS NOMOGRAM_IDs, extracts station names, determines utility from
constraint cause codes, and enriches the ranked congestion data.
Also classifies reconductoring applicability.
"""
import csv
import json
import os
import re
from collections import defaultdict

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RANKED_FILE = os.path.join(BASE_DIR, "data", "congestion_ranked.csv")
RAW_FILE = os.path.join(BASE_DIR, "data", "shadow_prices_12mo.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "congestion_enriched.csv")
KNOWN_MAPPINGS_FILE = os.path.join(BASE_DIR, "data", "known_mappings.json")

# Known mappings from DMM reports and CAISO TPP
KNOWN_MAPPINGS = {
    "30055_GATES1  _500_30060_MIDWAY  _500_BR_1 _1": {
        "name": "Gates-Midway 500 kV (Path 26)",
        "tpp_area": "Path 26 Corridor",
        "utility": "PG&E",
        "reconductoring": False,
        "reason": "500 kV, stability-limited",
    },
    "30790_PANOCHE _230_30900_GATES   _230_BR_2 _1": {
        "name": "Panoche-Gates 230 kV",
        "tpp_area": "PG&E Fresno 230 kV / Path 15 Corridor",
        "utility": "PG&E",
        "reconductoring": True,
        "reason": "230 kV branch in renewable-rich corridor",
    },
    "30750_MOSSLD  _230_30797_LASAGUIL_230_BR_1 _1": {
        "name": "Moss Landing-Las Aguilas 230 kV",
        "tpp_area": "Path 15 Corridor",
        "utility": "PG&E",
        "reconductoring": True,
        "reason": "230 kV, identified as DLR candidate in PG&E SB1006",
    },
    "22208_EL CAJON_69.0_22408_LOSCOCHS_69.0_BR_1 _1": {
        "name": "El Cajon-Los Coches 69 kV",
        "tpp_area": "SDG&E 230 kV",
        "utility": "SDG&E",
        "reconductoring": True,
        "reason": "69 kV branch, persistently congested",
    },
    "30040_TESLA   _500_30050_LOSBANOS_500_BR_1 _1": {
        "name": "Tesla-Los Banos 500 kV (Path 15)",
        "tpp_area": "Path 15 Corridor",
        "utility": "PG&E",
        "reconductoring": False,
        "reason": "500 kV, stability-limited",
    },
    "30060_MIDWAY  _500_24156_VINCENT _500_BR_2 _3": {
        "name": "Midway-Vincent 500 kV (Path 26)",
        "tpp_area": "Path 26 Corridor",
        "utility": "PG&E/SCE",
        "reconductoring": False,
        "reason": "500 kV, stability-limited",
    },
    "25201_LEWIS   _230_24137_SERRANO _230_BR_1 _1": {
        "name": "Lewis-Serrano 230 kV",
        "tpp_area": "SCE Northern",
        "utility": "SCE",
        "reconductoring": True,
        "reason": "230 kV branch in SCE service territory",
    },
    "33020_MORAGA  _115_35101_SN LNDRO_115_BR_2 _1": {
        "name": "Moraga-San Leandro 115 kV",
        "tpp_area": "PG&E Greater Bay Area",
        "utility": "PG&E",
        "reconductoring": True,
        "reason": "115 kV, identified for reconductoring in Oakland South Reinforcement (TPP 2024-25)",
    },
    "35618_SN JSE A_115_35620_EL PATIO_115_BR_1 _1": {
        "name": "San Jose A-El Patio 115 kV",
        "tpp_area": "PG&E Greater Bay Area",
        "utility": "PG&E",
        "reconductoring": True,
        "reason": "115 kV branch in Bay Area load pocket",
    },
    "30765_LOSBANOS_230_30790_PANOCHE _230_BR_2 _1": {
        "name": "Los Banos-Panoche 230 kV",
        "tpp_area": "Path 15 Corridor",
        "utility": "PG&E",
        "reconductoring": True,
        "reason": "230 kV in Path 15 corridor, solar congestion",
    },
    "34548_KETTLEMN_70.0_34552_GATES   _70.0_BR_1 _1": {
        "name": "Kettleman-Gates 70 kV",
        "tpp_area": "PG&E Kern / Fresno",
        "utility": "PG&E",
        "reconductoring": True,
        "reason": "70 kV branch in solar-rich zone, thermally limited",
    },
    "34116_LE GRAND_115_34115_ADRA TAP_115_BR_1 _1": {
        "name": "Le Grand-Adra Tap 115 kV",
        "tpp_area": "PG&E Fresno",
        "utility": "PG&E",
        "reconductoring": True,
        "reason": "115 kV branch in central valley",
    },
    "30765_LOSBANOS_230_30766_PADR FLT_230_BR_1 _1": {
        "name": "Los Banos-Padre Flat 230 kV",
        "tpp_area": "Path 15 Corridor",
        "utility": "PG&E",
        "reconductoring": True,
        "reason": "230 kV in Path 15 corridor",
    },
    "34214_LOS BANS_70.0_30765_LOSBANOS_230_XF_3": {
        "name": "Los Banos 70/230 kV Transformer",
        "tpp_area": "Path 15 Corridor",
        "utility": "PG&E",
        "reconductoring": False,
        "reason": "Transformer, not a transmission line",
    },
    "31486_CARIBOU _115_30255_CARBOU M_ 1.0_XF_11": {
        "name": "Caribou 115/1 kV Transformer (Generator)",
        "tpp_area": "PG&E Sierra",
        "utility": "PG&E",
        "reconductoring": False,
        "reason": "Transformer/generator step-up",
    },
    "32214_RIO OSO _115_32225_BRNSWKT1_115_BR_1 _1": {
        "name": "Rio Oso-Brunswick T1 115 kV",
        "tpp_area": "PG&E Sierra / North Valley",
        "utility": "PG&E",
        "reconductoring": True,
        "reason": "115 kV branch",
    },
    "34418_KINGSBRG_115_34428_CONTADNA_115_BR_1 _1": {
        "name": "Kingsburg-Contadina 115 kV",
        "tpp_area": "PG&E Fresno",
        "utility": "PG&E",
        "reconductoring": True,
        "reason": "115 kV branch in Fresno area",
    },
    "34101_CERTANJ2_115_34116_LE GRAND_115_BR_1 _1": {
        "name": "Certaneja 2-Le Grand 115 kV",
        "tpp_area": "PG&E Fresno",
        "utility": "PG&E",
        "reconductoring": True,
        "reason": "115 kV branch",
    },
    "24602_VICTOR  _115_24607_ROADWAY _115_BR_1 _1": {
        "name": "Victor-Roadway 115 kV",
        "tpp_area": "SCE Eastern",
        "utility": "SCE",
        "reconductoring": True,
        "reason": "115 kV branch, solar-driven congestion (80% solar hours)",
    },
    "24791_TAP710  _115_24731_INYOKERN_115_BR_2 _1": {
        "name": "Tap710-Inyokern 115 kV",
        "tpp_area": "SCE Eastern / Kern",
        "utility": "SCE",
        "reconductoring": True,
        "reason": "115 kV in solar/wind zone",
    },
    "39021_SC21ATP _70.0_34888_ARVIN   _70.0_BR_1 _1": {
        "name": "SC21A Tap-Arvin 70 kV",
        "tpp_area": "PG&E Kern",
        "utility": "PG&E",
        "reconductoring": True,
        "reason": "70 kV branch, #1 most congested line, thermally limited",
    },
    "31336_HPLND JT_60.0_31370_CLVRDLJT_60.0_BR_1 _1": {
        "name": "Highland Junction-Cloverdale Junction 60 kV",
        "tpp_area": "PG&E North Coast",
        "utility": "PG&E",
        "reconductoring": True,
        "reason": "60 kV branch, #2 most congested, likely thermally limited",
    },
    "24723_CONTROL _115_24865_TAP188  _115_BR_2 _1": {
        "name": "Control-Tap188 115 kV",
        "tpp_area": "SCE Eastern",
        "utility": "SCE",
        "reconductoring": True,
        "reason": "115 kV branch, #3 most congested, base case binding",
    },
    "24723_CONTROL _115_24866_TAP189  _115_BR_1 _1": {
        "name": "Control-Tap189 115 kV",
        "tpp_area": "SCE Eastern",
        "utility": "SCE",
        "reconductoring": True,
        "reason": "115 kV branch, same corridor as Control-Tap188",
    },
}

# Station abbreviation to full name mapping (common OASIS abbreviations)
STATION_NAMES = {
    "MOSSLD": "Moss Landing",
    "LOSBANOS": "Los Banos",
    "LOS BANS": "Los Banos",
    "LOSCOCHS": "Los Coches",
    "INYOKERN": "Inyokern",
    "PENSQTOS": "Penasquitos",
    "PQUTOS": "Penasquitos",
    "KETTLEMN": "Kettleman",
    "EL CAJON": "El Cajon",
    "PANOCHE": "Panoche",
    "GATES": "Gates",
    "GATES1": "Gates",
    "MIDWAY": "Midway",
    "TESLA": "Tesla",
    "VINCENT": "Vincent",
    "VINCNT": "Vincent",
    "MORAGA": "Moraga",
    "SN LNDRO": "San Leandro",
    "SN JSE A": "San Jose A",
    "EL PATIO": "El Patio",
    "CONTROL": "Control",
    "LASAGUIL": "Las Aguilas",
    "PARDEE": "Pardee",
    "SYLMAR S": "Sylmar South",
    "SERRANO": "Serrano",
    "LEWIS": "Lewis",
    "OTAY": "Otay",
    "OTAYLKTP": "Otay Lakes Tap",
    "CARIBOU": "Caribou",
    "CARBOU M": "Caribou Main",
    "RIO OSO": "Rio Oso",
    "BRNSWKT1": "Brunswick T1",
    "KINGSBRG": "Kingsburg",
    "CONTADNA": "Contadina",
    "LE GRAND": "Le Grand",
    "CERTANJ2": "Certaneja 2",
    "VICTOR": "Victor",
    "ROADWAY": "Roadway",
    "ARVIN": "Arvin",
    "SC21ATP": "SC21A Tap",
    "HPLND JT": "Highland Junction",
    "CLVRDLJT": "Cloverdale Junction",
    "TAP710": "Tap 710",
    "TAP188": "Tap 188",
    "TAP189": "Tap 189",
    "ADRA TAP": "Adra Tap",
    "PADR FLT": "Padre Flat",
    "DEVERS": "Devers",
    "LUGO": "Lugo",
    "MNTVIS": "Mountain View",
    "METCLF": "Metcalf",
    "EASTSH": "East Shore",
    "SANMAT": "San Mateo",
    "NEWARK": "Newark",
    "PITTSP": "Pittsburg",
    "MIGUEL": "Miguel",
    "SLVRGT": "Silvergate",
    "SXCYN": "Sycamore Canyon",
    "SCRIPS": "Scripps",
    "SNCRST": "Sunrise Crest",
    "IVALLY": "Imperial Valley",
    "RNDMTN": "Round Mountain",
    "TBLMTN": "Table Mountain",
    "COLGAT": "Colgate",
    "RIOOSO": "Rio Oso",
    "DRUM": "Drum",
    "PALRMO": "Palermo",
    "CORTIN": "Cortin",
    "CTTNWD": "Cottonwood",
    "NRTHRN": "Northern",
    "WHIRWD": "Whirlwind",
    "ANTLPE": "Antelope",
    "WINHUB": "Windhub",
    "PVERDE": "Palo Verde",
    "COLRIV": "Colorado River",
    "MESAS": "Mesa",
    "BIGCRK3": "Big Creek 3",
    "RECTOR": "Rector",
    "KRAMER": "Kramer",
    "CWATER": "Coolwater",
    "IVANPA": "Ivanpah",
    "MAGNDN": "Magnolia/Minden",
    "PASTRA": "Pastoria",
    "LAFRES": "La Fresa",
    "LAGBEL": "Laguna Bell",
    "MOORPK": "Moorpark",
    "SNCLRA": "Santa Clara",
    "BAYBLV": "Bay Boulevard",
    "OLDTWN": "Old Town",
    "ESCNDO": "Escondido",
    "LILIAC": "Lilac",
    "PALA": "Pala",
    "PNDLTN": "Pendleton",
    "SANLUS": "San Luis Rey",
    "MNSRTE": "Monserate",
    "FELCTA": "Felicita",
    "VLCNTR": "Valley Center",
    "SOBRNT": "Sobrante",
    "LOCKFD": "Lockford",
    "CLRMTK": "Claremont K",
    "OAK D": "Oakland D",
    "OAK C": "Oakland C",
    "OAK L": "Oakland L",
    "OAK X": "Oakland X",
    "IGNACO": "Ignacio",
    "SANRAF": "San Rafael",
    "LAKVIL": "Lakeville",
    "VACADX": "Vacaville DX",
    "LMBEPK": "Lambie Park",
    "MARTIN": "Martin",
    "JEFRSN": "Jefferson",
    "TRNQSS": "Tranquess",
    "KERNEY": "Kearney",
    "SANGER": "Sanger",
    "MALAGA": "Malaga",
    "MCCALL": "McCall",
    "MSTANS": "Mustang",
    "WILSON": "Wilson",
    "LGRAND": "Le Grand",
    "MANTCA": "Manteca",
    "MANTEC": "Manteca",
    "SCHLTE": "Schulte",
    "STANIS": "Stanislaus",
    "MELONS": "Melones",
    "LOSBNS": "Los Banos",
    "BELOTA": "Belota",
    "DAIRLD": "Dairyland",
    "MNDOTA": "Mendota",
    "PNOCHE": "Panoche",
    "EXCLSS": "Excelsior Springs",
    "SANLOB": "San Luis Obispo",
    "TEMBLR": "Temblor",
    "KERNPP": "Kern Power Plant",
    "WESTPK": "Westpark",
    "WHELRG": "Wheeler Ridge",
    "WEEDPC": "Weedpatch",
    "BRDSLD": "Birdsall",
    "COCOPP": "Coconut P",
    "LASPOS": "Las Positas",
    "TAFT": "Taft",
    "HYATT": "Hyatt",
    "SCOTT": "Scott",
    "NWRKDST": "Newark District",
    "RAVENS": "Ravenswood",
    "KELSO": "Kelso",
    "BRNTWD": "Brentwood",
    "LOSEST": "Los Esteros",
    "ELPATO": "El Patio",
    "HIGGNS": "Higgins",
    "BELL": "Bell",
    "ECONTY": "East County",
    "SALTCK": "Salt Creek",
    "JMACHA": "Jamacha",
    "STA B": "Station B",
    "LUDLO": "Ludlow",
    "MOHV1": "Mohave",
    "PAHRUM": "Pahrump",
    "VEAVST": "VEA Vest",
    "SVPSSS": "SVP SSS",
}

# County/utility to TPP area mapping
COUNTY_UTILITY_TO_TPP = {
    "Kern_PG&E": "PG&E Kern 230 kV",
    "Kern_SCE": "SCE Eastern",
    "Fresno_PG&E": "PG&E Fresno",
    "Kings_PG&E": "PG&E Fresno",
    "Merced_PG&E": "Path 15 Corridor",
    "San Benito_PG&E": "Path 15 Corridor",
    "Monterey_PG&E": "PG&E Morro Bay 230 kV",
    "San Luis Obispo_PG&E": "PG&E Morro Bay 230 kV",
    "Alameda_PG&E": "PG&E Greater Bay Area",
    "Contra Costa_PG&E": "PG&E Greater Bay Area",
    "Santa Clara_PG&E": "PG&E Greater Bay Area",
    "San Mateo_PG&E": "PG&E Greater Bay Area",
    "San Francisco_PG&E": "PG&E Greater Bay Area",
    "Placer_PG&E": "PG&E Sierra",
    "El Dorado_PG&E": "PG&E Sierra",
    "Shasta_PG&E": "PG&E North Valley 230 kV",
    "Butte_PG&E": "PG&E North Valley 230 kV",
    "San Diego_SDG&E": "SDG&E 230 kV",
    "Imperial_SDG&E": "SDG&E Bulk",
    "Los Angeles_SCE": "SCE Northern / SCE Metro",
    "San Bernardino_SCE": "SCE North of Lugo / East of Pisgah",
    "Riverside_SCE": "SCE Eastern",
}


def parse_nomogram_id(nid):
    """Parse an OASIS NOMOGRAM_ID into structured components."""
    # Detect nomograms
    if nid.endswith("_NG") or not re.search(r"_BR_|_XF_", nid):
        return {
            "raw_id": nid,
            "is_nomogram": True,
            "station_a": None,
            "station_b": None,
            "voltage_kv": None,
            "element_type": "nomogram",
            "nomogram_name": nid,
        }

    is_xf = "_XF_" in nid
    element_type = "transformer" if is_xf else "branch"

    if is_xf:
        parts = nid.split("_XF_")
    else:
        parts = nid.split("_BR_")

    endpoint_str = parts[0] if parts else nid
    tokens = endpoint_str.split("_")
    tokens = [t for t in tokens if t.strip()]

    kv_values = [60, 60.0, 66, 69, 69.0, 70, 70.0, 115, 138, 220, 230, 345, 500, 765, 1.0]

    stations = []
    voltages = []
    current_station = []

    for token in tokens:
        token_clean = token.strip()
        try:
            val = float(token_clean)
            if val in kv_values or (val > 60 and val < 800 and val == int(val)):
                if current_station:
                    stations.append(" ".join(current_station))
                    current_station = []
                voltages.append(val)
            elif val > 10000:
                if current_station:
                    stations.append(" ".join(current_station))
                    current_station = []
            else:
                current_station.append(token_clean)
        except ValueError:
            current_station.append(token_clean)

    if current_station:
        stations.append(" ".join(current_station))

    return {
        "raw_id": nid,
        "is_nomogram": False,
        "station_a": stations[0] if len(stations) > 0 else None,
        "station_b": stations[1] if len(stations) > 1 else None,
        "voltage_kv": max(voltages) if voltages else None,
        "element_type": element_type,
    }


def resolve_station_name(abbrev):
    """Resolve OASIS station abbreviation to full name."""
    if not abbrev:
        return None
    clean = abbrev.strip()
    return STATION_NAMES.get(clean, clean.title())


def determine_utility_from_causes(nomogram_id, raw_data_causes):
    """Determine utility from constraint cause prefixes."""
    causes = raw_data_causes.get(nomogram_id, {})
    if not causes:
        return "Unknown"

    utility_counts = defaultdict(int)
    for cause, count in causes.items():
        if cause.startswith("PG1") or cause.startswith("PG2"):
            utility_counts["PG&E"] += count
        elif cause.startswith("SC1") or cause.startswith("SC2"):
            utility_counts["SCE"] += count
        elif cause.startswith("SD1") or cause.startswith("SD2"):
            utility_counts["SDG&E"] += count
        elif cause.startswith("SV1"):
            utility_counts["SVP"] += count
        elif cause.startswith("VE1"):
            utility_counts["VEA"] += count
        elif cause == "Base Case":
            pass  # no utility info

    if utility_counts:
        return max(utility_counts, key=utility_counts.get)

    # Fallback: try to infer from station names in nomogram_id
    nid_upper = nomogram_id.upper()
    if any(s in nid_upper for s in ["GATES", "LOSBANOS", "PANOCHE", "MOSSLD", "TESLA", "MORAGA", "METCLF"]):
        return "PG&E"
    if any(s in nid_upper for s in ["VINCENT", "SERRANO", "PARDEE", "SYLMAR", "DEVERS", "LUGO"]):
        return "SCE"
    if any(s in nid_upper for s in ["MIGUEL", "OTAY", "EL CAJON", "LOSCOCHS", "SLVRGT"]):
        return "SDG&E"

    return "Unknown"


def build_cause_lookup():
    """Build a lookup of all causes per nomogram_id from raw data."""
    causes = defaultdict(lambda: defaultdict(int))
    with open(RAW_FILE) as f:
        reader = csv.DictReader(f)
        for row in reader:
            nid = row["NOMOGRAM_ID"]
            cause = row["CONSTRAINT_CAUSE"]
            causes[nid][cause] += 1
    return causes


def main():
    # Build cause lookup from raw data
    print("Building cause lookup from raw data...")
    raw_causes = build_cause_lookup()

    # Load ranked constraints
    constraints = []
    with open(RANKED_FILE) as f:
        reader = csv.DictReader(f)
        for row in reader:
            constraints.append(row)
    print(f"Processing {len(constraints)} constraints...")

    enriched = []
    match_stats = {"known": 0, "parsed": 0, "nomogram": 0}

    for c in constraints:
        nid = c["nomogram_id"]

        # Check known mappings first
        if nid in KNOWN_MAPPINGS:
            km = KNOWN_MAPPINGS[nid]
            c["readable_name"] = km["name"]
            c["utility"] = km.get("utility", determine_utility_from_causes(nid, raw_causes))
            c["tpp_area"] = km.get("tpp_area", "")
            c["reconductoring_applicable"] = km["reconductoring"]
            c["reconductoring_reason"] = km["reason"]
            c["match_quality"] = "known_mapping"
            match_stats["known"] += 1
            enriched.append(c)
            continue

        # Parse the nomogram ID
        parsed = parse_nomogram_id(nid)

        if parsed["is_nomogram"]:
            c["readable_name"] = parsed["nomogram_name"]
            c["utility"] = determine_utility_from_causes(nid, raw_causes)
            c["tpp_area"] = ""
            c["reconductoring_applicable"] = False
            c["reconductoring_reason"] = "Nomogram (multi-element constraint), not a physical line"
            c["match_quality"] = "nomogram"
            match_stats["nomogram"] += 1
            enriched.append(c)
            continue

        # Build readable name from parsed station names
        name_a = resolve_station_name(parsed["station_a"]) or "?"
        name_b = resolve_station_name(parsed["station_b"]) or "?"
        voltage = parsed["voltage_kv"]
        if parsed["element_type"] == "transformer":
            # For transformers, show both voltages
            readable = f"{name_a} {int(voltage) if voltage else '?'} kV Transformer"
        else:
            readable = f"{name_a}-{name_b} {int(voltage) if voltage else '?'} kV"

        c["readable_name"] = readable
        c["utility"] = determine_utility_from_causes(nid, raw_causes)
        c["tpp_area"] = ""

        # Determine reconductoring applicability
        v = voltage or 0
        is_branch = parsed["element_type"] == "branch"

        if not is_branch:
            c["reconductoring_applicable"] = False
            c["reconductoring_reason"] = "Transformer, not a transmission line"
        elif v >= 500:
            c["reconductoring_applicable"] = False
            c["reconductoring_reason"] = f"{int(v)} kV line - likely stability-limited, reconductoring won't increase capacity"
        elif v <= 230 and is_branch:
            c["reconductoring_applicable"] = True
            c["reconductoring_reason"] = f"{int(v)} kV branch, likely thermally limited - reconductoring candidate"
        else:
            c["reconductoring_applicable"] = None
            c["reconductoring_reason"] = "Insufficient data to determine"

        c["match_quality"] = "parsed"
        match_stats["parsed"] += 1
        enriched.append(c)

    # Determine renewable-driven from solar_hour_fraction
    for c in enriched:
        solar_frac = float(c.get("solar_hour_fraction", 0))
        if solar_frac > 0.6:
            c["renewable_driven"] = True
            c["renewable_type"] = "solar"
        elif solar_frac > 0.4:
            c["renewable_driven"] = "likely"
            c["renewable_type"] = "mixed"
        else:
            c["renewable_driven"] = False
            c["renewable_type"] = "load/other"

    # Write enriched output
    fieldnames = list(enriched[0].keys())
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched)

    # Save known mappings
    with open(KNOWN_MAPPINGS_FILE, "w") as f:
        json.dump(KNOWN_MAPPINGS, f, indent=2)

    # Report
    recon_yes = sum(1 for e in enriched if e.get("reconductoring_applicable") == True or e.get("reconductoring_applicable") == "True")
    recon_no = sum(1 for e in enriched if e.get("reconductoring_applicable") == False or e.get("reconductoring_applicable") == "False")
    renewable = sum(1 for e in enriched if e.get("renewable_driven") == True)

    print(f"\nResults:")
    print(f"  Known mappings used: {match_stats['known']}")
    print(f"  Parsed from ID: {match_stats['parsed']}")
    print(f"  Nomograms: {match_stats['nomogram']}")
    print(f"  Reconductoring candidates: {recon_yes}")
    print(f"  Not applicable for reconductoring: {recon_no}")
    print(f"  Renewable-driven constraints: {renewable}")

    print(f"\nTop 25 by congestion cost index:")
    print(f"{'Rank':<5} {'Name':<45} {'kV':>5} {'Utility':<8} {'Cost Index':>12} {'Hours':>6} {'Recon?':>6} {'Solar%':>7}")
    print("-" * 100)
    for i, r in enumerate(enriched[:25], 1):
        kv = r['voltage_kv'] if r['voltage_kv'] else '?'
        recon = 'Yes' if r['reconductoring_applicable'] in [True, 'True'] else 'No'
        solar = float(r.get('solar_hour_fraction', 0))
        print(f"{i:<5} {r['readable_name'][:44]:<45} {kv:>5} {r['utility']:<8} {float(r['total_congestion_cost_index']):>12.0f} {r['hours_binding']:>6} {recon:>6} {solar:>6.1%}")


if __name__ == "__main__":
    main()
