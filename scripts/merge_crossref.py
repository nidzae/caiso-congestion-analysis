"""
merge_crossref.py
Merges the enriched congestion data with the cross-reference intervention data
into a final JSON dataset for the visualization.
"""
import csv
import json
import os

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
ENRICHED_FILE = os.path.join(BASE_DIR, "data", "congestion_enriched.csv")
CROSSREF_FILE = os.path.join(BASE_DIR, "data", "cross_reference.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "visualization_data.json")


# Estimated typical thermal ratings (MW) by voltage class for single-circuit lines.
# Sources: CAISO Full Network Model summary data, WECC path rating catalog,
# and standard engineering references (Glover/Sarma/Overbye Power Systems Analysis).
# These are NORMAL ratings (not emergency). Actual ratings vary by conductor type,
# ambient temperature, wind, span length, and altitude.
# We use conservative midpoint estimates for California conditions.
ESTIMATED_THERMAL_RATING_MW = {
    60: 75,     # 60 kV: typical 50-100 MW single circuit
    66: 85,     # 66 kV: similar to 60 kV
    69: 100,    # 69 kV: typical 75-150 MW
    70: 100,    # 70 kV: same class as 69 kV
    115: 250,   # 115 kV: typical 150-400 MW
    138: 400,   # 138 kV: typical 300-500 MW
    220: 600,   # 220 kV: typical 400-800 MW
    230: 700,   # 230 kV: typical 500-1000 MW
    345: 1200,  # 345 kV: typical 900-1500 MW
    500: 2000,  # 500 kV: typical 1500-3000 MW
    765: 3500,  # 765 kV: typical 2500-4500 MW
}

# When a constraint is binding, flow is typically below the thermal limit because
# operating limits include contingency margins (N-1 security), stability limits,
# and voltage constraints. Industry practice and CAISO operating data suggest
# constrained flow is typically 60-80% of the thermal rating. We use 70% as
# a midpoint estimate.
BINDING_FLOW_FRACTION = 0.70


def estimate_flow_mw(voltage_kv, element_type):
    """Estimate the binding flow in MW for a given voltage class.

    Returns the thermal rating derated by BINDING_FLOW_FRACTION to approximate
    the actual constrained flow when a line is binding.
    """
    if voltage_kv is None:
        return None, None
    v = round(voltage_kv)
    # Find match in our table
    if v in ESTIMATED_THERMAL_RATING_MW:
        thermal = ESTIMATED_THERMAL_RATING_MW[v]
    else:
        closest = min(ESTIMATED_THERMAL_RATING_MW.keys(), key=lambda x: abs(x - v))
        if abs(closest - v) <= 10:
            thermal = ESTIMATED_THERMAL_RATING_MW[closest]
        else:
            return None, None
    binding_flow = round(thermal * BINDING_FLOW_FRACTION)
    return thermal, binding_flow


def main():
    # Load cross-reference
    with open(CROSSREF_FILE) as f:
        crossref = json.load(f)

    interventions = crossref.get("interventions", {})
    tpp_areas = crossref.get("tpp_congestion_areas", {})

    # Load enriched congestion data
    records = []
    with open(ENRICHED_FILE) as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)

    # Merge
    merged = []
    for r in records:
        nid = r["nomogram_id"]
        interv = interventions.get(nid, {})

        # Get TPP area info
        tpp_area = interv.get("tpp_area", r.get("tpp_area", ""))
        tpp_info = tpp_areas.get(tpp_area, {})

        # Determine constraint type from cause
        cause = r.get("primary_cause", "")
        cause_is_base = r.get("cause_is_base_case", "False") == "True"
        if cause_is_base:
            constraint_type = "thermal"
        elif "500" in cause:
            constraint_type = "stability"
        elif "_M" in cause:
            constraint_type = "contingency"
        else:
            constraint_type = "contingency"

        # Parse booleans
        recon = r.get("reconductoring_applicable")
        if recon == "True":
            recon = True
        elif recon == "False":
            recon = False
        else:
            recon = None

        renewable = r.get("renewable_driven")
        if renewable == "True":
            renewable = True
        elif renewable == "False":
            renewable = False
        else:
            renewable = str(renewable) == "likely" if renewable else False

        voltage = r.get("voltage_kv")
        try:
            voltage = float(voltage) if voltage else None
        except (ValueError, TypeError):
            voltage = None

        # Determine if this constraint was manually researched
        was_researched = nid in interventions

        entry = {
            "id": nid,
            "name": r.get("readable_name", nid),
            "voltage_kv": voltage,
            "utility": r.get("utility", "Unknown"),
            "congestion_area": tpp_area,
            "cost_index": round(float(r.get("total_congestion_cost_index", 0))),
            "hours_binding": int(r.get("hours_binding", 0)),
            "days_binding": int(r.get("days_binding", 0)),
            "avg_shadow_price": float(r.get("avg_shadow_price", 0)),
            "max_shadow_price": float(r.get("max_shadow_price", 0)),
            "months_active": int(r.get("months_active", 0)),
            "primary_cause": cause,
            "constraint_type": constraint_type,
            "renewable_driven": renewable,
            "renewable_type": r.get("renewable_type", "unknown"),
            "solar_hour_fraction": float(r.get("solar_hour_fraction", 0)),
            "reconductoring_applicable": recon,
            "reconductoring_reason": r.get("reconductoring_reason", ""),
            "element_type": r.get("element_type", "branch"),
            "intervention_proposed": interv.get("intervention_proposed", False) if was_researched else "not_searched",
            "intervention_type": interv.get("intervention_type", "none") if was_researched else "not_searched",
            "intervention_source": interv.get("intervention_source", "none") if was_researched else "not_searched",
            "intervention_approved": interv.get("intervention_approved", None) if was_researched else "not_searched",
            "rejection_reason": interv.get("rejection_reason", None),
            "tpp_congestion_cost_M": interv.get("tpp_congestion_cost_M", tpp_info.get("cost_2039_M")),
            "notes": interv.get("notes", "") if was_researched else "Intervention status not researched for this constraint.",
        }

        # Estimated congestion cost:
        # Congestion rent per hour ≈ shadow_price ($/MWh) × flow_on_constraint (MW)
        # When binding, flow ≈ thermal_rating × BINDING_FLOW_FRACTION (typically 70%)
        # So: estimated_cost ≈ cost_index × thermal_rating × 0.70
        thermal_mw, binding_mw = estimate_flow_mw(voltage, r.get("element_type", "branch"))
        entry["estimated_thermal_rating_mw"] = thermal_mw
        entry["estimated_flow_mw"] = binding_mw
        if binding_mw is not None and entry["cost_index"] != 0:
            est_cost = entry["cost_index"] * binding_mw
            entry["estimated_cost"] = round(est_cost)
            entry["estimated_cost_M"] = round(est_cost / 1_000_000, 2)
        else:
            entry["estimated_cost"] = None
            entry["estimated_cost_M"] = None

        merged.append(entry)

    # Sort by cost_index descending
    merged.sort(key=lambda x: x["cost_index"], reverse=True)

    # Write output
    with open(OUTPUT_FILE, "w") as f:
        json.dump(merged, f, indent=2)

    # Summary stats
    total = len(merged)
    researched = sum(1 for m in merged if m["intervention_proposed"] != "not_searched")
    not_searched = total - researched
    recon_applicable = sum(1 for m in merged if m["reconductoring_applicable"] == True)
    recon_proposed = sum(1 for m in merged if m["intervention_proposed"] not in (False, "not_searched") and m["intervention_proposed"] and m["reconductoring_applicable"] == True)
    recon_approved = sum(1 for m in merged if m["intervention_approved"] == True and m["reconductoring_applicable"] == True)
    gap_researched = sum(1 for m in merged if m["reconductoring_applicable"] == True and m["intervention_proposed"] == False)
    renewable_count = sum(1 for m in merged if m["renewable_driven"] == True)

    print(f"Merged {total} constraints into {OUTPUT_FILE}")
    print(f"\nSummary:")
    print(f"  Total binding constraints: {total}")
    print(f"  Manually researched: {researched}")
    print(f"  Not searched: {not_searched}")
    print(f"  Reconductoring applicable: {recon_applicable}")
    print(f"  Reconductoring proposed (among researched): {recon_proposed}")
    print(f"  Reconductoring approved: {recon_approved}")
    print(f"  GAP (researched, applicable, not proposed): {gap_researched}")
    print(f"  Renewable-driven: {renewable_count}")


if __name__ == "__main__":
    main()
