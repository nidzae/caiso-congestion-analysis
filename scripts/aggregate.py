"""
aggregate.py
Reads the consolidated 12-month shadow price CSV and produces a
stack-ranked table of the most costly constrained transmission elements.
"""
import csv
import os
from collections import defaultdict

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
INPUT_FILE = os.path.join(BASE_DIR, "data", "shadow_prices_12mo.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "congestion_ranked.csv")


def parse_nomogram_id(nid):
    """Extract human-readable info from NOMOGRAM_ID."""
    parts = nid.split("_")
    info = {
        "raw_id": nid,
        "element_type": "transformer" if "_XF_" in nid else "branch",
    }

    # Try to extract voltage
    voltages = []
    for p in parts:
        p_clean = p.strip()
        try:
            v = float(p_clean)
            if v in [66, 69, 69.0, 70, 70.0, 115, 138, 220, 230, 345, 500, 765]:
                voltages.append(v)
        except ValueError:
            pass
    info["voltage_kv"] = max(voltages) if voltages else None

    return info


def main():
    # First pass: collect all rows per constraint, grouped by (date, hour)
    # A constraint can bind for multiple causes in the same hour (different GROUP).
    # We take the max PRC per (constraint, date, hour) and count it as one hour.
    raw_by_constraint = defaultdict(list)

    with open(INPUT_FILE) as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_by_constraint[row["NOMOGRAM_ID"]].append(row)

    total_raw_rows = sum(len(v) for v in raw_by_constraint.values())
    print(f"Read {total_raw_rows} raw rows for {len(raw_by_constraint)} constraints")

    # Second pass: deduplicate by (date, hour), taking max PRC per hour
    constraints = {}
    total_deduped = 0

    for nid, rows in raw_by_constraint.items():
        # Group by (date, hour)
        by_hour = defaultdict(list)
        for row in rows:
            key = (row["OPR_DT"], int(row["OPR_HR"]))
            by_hour[key].append(row)

        hours_binding = 0
        total_shadow_price = 0.0
        max_shadow_price = 0.0
        causes = defaultdict(int)
        dates_binding = set()
        hours_by_month = defaultdict(int)
        hours_list = []

        for (opr_dt, opr_hr), hour_rows in by_hour.items():
            # Take max PRC across all causes for this hour
            best_row = max(hour_rows, key=lambda r: float(r["PRC"]))
            prc = float(best_row["PRC"])

            hours_binding += 1
            total_shadow_price += prc
            max_shadow_price = max(max_shadow_price, prc)
            dates_binding.add(opr_dt)
            hours_by_month[opr_dt[:7]] += 1
            hours_list.append(opr_hr)

            # Count all causes seen this hour (for primary cause determination)
            for r in hour_rows:
                causes[r["CONSTRAINT_CAUSE"]] += 1

        constraints[nid] = {
            "hours_binding": hours_binding,
            "total_shadow_price": total_shadow_price,
            "max_shadow_price": max_shadow_price,
            "causes": causes,
            "dates_binding": dates_binding,
            "hours_by_month": hours_by_month,
            "hours_list": hours_list,
        }
        total_deduped += hours_binding

    print(f"After dedup: {total_deduped} constraint-hours (was {total_raw_rows} raw rows, removed {total_raw_rows - total_deduped} multi-cause duplicates)")

    # Calculate derived metrics
    results = []
    for nid, data in constraints.items():
        info = parse_nomogram_id(nid)
        avg_price = data["total_shadow_price"] / data["hours_binding"]
        primary_cause = max(data["causes"], key=data["causes"].get)
        days_binding = len(data["dates_binding"])

        # Classify solar hours (9-16 Pacific)
        solar_hours = sum(1 for h in data["hours_list"] if 9 <= h <= 16)
        solar_fraction = solar_hours / len(data["hours_list"]) if data["hours_list"] else 0

        results.append({
            "nomogram_id": nid,
            "voltage_kv": info["voltage_kv"],
            "element_type": info["element_type"],
            "total_congestion_cost_index": data["total_shadow_price"],
            "hours_binding": data["hours_binding"],
            "days_binding": days_binding,
            "avg_shadow_price": round(avg_price, 2),
            "max_shadow_price": round(data["max_shadow_price"], 2),
            "primary_cause": primary_cause,
            "cause_is_base_case": primary_cause == "Base Case",
            "months_active": len(data["hours_by_month"]),
            "solar_hour_fraction": round(solar_fraction, 3),
        })

    # Sort by total congestion cost index (descending)
    results.sort(key=lambda x: x["total_congestion_cost_index"], reverse=True)

    # Write output
    fieldnames = list(results[0].keys())
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"Ranked {len(results)} constraints. Top 20:")
    print(f"{'Rank':<5} {'Nomogram ID':<55} {'kV':>5} {'Cost Index':>12} {'Hours':>6} {'Avg $/MWh':>10} {'Solar%':>7} {'Primary Cause':<30}")
    print("-" * 135)
    for i, r in enumerate(results[:20], 1):
        kv = int(r['voltage_kv']) if r['voltage_kv'] else '?'
        print(f"{i:<5} {r['nomogram_id'][:54]:<55} {kv:>5} {r['total_congestion_cost_index']:>12.0f} {r['hours_binding']:>6} {r['avg_shadow_price']:>10.1f} {r['solar_hour_fraction']:>6.1%} {r['primary_cause'][:29]:<30}")


if __name__ == "__main__":
    main()
