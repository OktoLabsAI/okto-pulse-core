"""One-shot fix: reverse the AC split caused by pipe in update_spec.

Spec cc85f92d had AC "MCP velocity aceita granularity=day|week params equivalente
ao REST" which update_spec's pipe-separator tokenized into two ACs, corrupting
indices for downstream scenarios.

This script:
1. Finds the two split fragments by text match.
2. Merges them back into a single AC (replacing `=` with ` ` to avoid re-splits).
3. Rewrites spec.acceptance_criteria to 11 entries.
4. Walks spec.test_scenarios[*].linked_criteria and remaps any stored text
   referencing the old fragments to the canonical index 7.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

DB = Path(r"C:/Users/jpamb/.okto-pulse/data/pulse.db")
SPEC_ID = "cc85f92d-07cf-430e-bc0e-3f29952968d2"

FRAG_A = "MCP velocity aceita granularity=day"
FRAG_B = "week params equivalente ao REST"
MERGED = "MCP velocity aceita granularity day week params equivalente ao REST"


def main() -> None:
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    row = con.execute(
        "SELECT acceptance_criteria, test_scenarios FROM specs WHERE id = ?",
        (SPEC_ID,),
    ).fetchone()
    assert row is not None, "spec not found"

    acs = json.loads(row["acceptance_criteria"] or "[]")
    scenarios = json.loads(row["test_scenarios"] or "[]")

    print(f"Before: ac_total={len(acs)}")
    try:
        idx_a = acs.index(FRAG_A)
        idx_b = acs.index(FRAG_B)
    except ValueError:
        print("Fragments not found — nothing to do")
        con.close()
        return
    assert idx_a + 1 == idx_b, f"fragments not consecutive: {idx_a}, {idx_b}"

    new_acs = acs[:idx_a] + [MERGED] + acs[idx_b + 1 :]
    print(f"After:  ac_total={len(new_acs)}")

    fixed_scenarios = 0
    for ts in scenarios:
        lc = ts.get("linked_criteria") or []
        if not lc:
            continue
        new_lc = []
        touched = False
        for entry in lc:
            if entry == FRAG_A or entry == FRAG_B:
                new_lc.append(idx_a)
                touched = True
            else:
                new_lc.append(entry)
        if touched:
            seen = []
            for e in new_lc:
                if e not in seen:
                    seen.append(e)
            ts["linked_criteria"] = seen
            fixed_scenarios += 1

    con.execute(
        "UPDATE specs SET acceptance_criteria = ?, test_scenarios = ? WHERE id = ?",
        (json.dumps(new_acs, ensure_ascii=False), json.dumps(scenarios, ensure_ascii=False), SPEC_ID),
    )
    con.commit()
    con.close()
    print(f"Fixed: merged AC at index {idx_a}; remapped {fixed_scenarios} scenario(s)")


if __name__ == "__main__":
    main()
