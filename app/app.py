from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import re
import uvicorn

# ======= Placeholders: table mappings and field lists =======
TABLE_MAP = {
    "MARC": "NSDM_V_MARC",
    "MARD": "NSDM_V_MARD",
    "KONV": "PRCD_ELEMENTS",
    "J_1BBRANCH": "P_BUSINESSPLACE",
    "J_1IMOVEND": "LFA1",
    "J_1IMOCUST": "KNA1",
    # Add further old->new mappings as needed
}

SUGGESTED_FIELDS = {
    "NSDM_V_MARC": ["MATNR", "WERKS"],
    "NSDM_V_MARD": ["MATNR", "WERKS", "LGORT"],
    "PRCD_ELEMENTS": ["KNUMV","KPOSN","STUNR","ZAEHK"],
    "P_BUSINESSPLACE": ["BUKRS,BRANCH"],
    "LFA1": ["LIFNR"],
    "KNA1": ["KUNNR"],
    # Expand field recommendations as needed
}

# --------- Pydantic Models --------------
class Unit(BaseModel):
    pgm_name: str
    inc_name: str
    type: str
    name: Optional[str] = None
    class_implementation: Optional[str] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    code: Optional[str] = ""

class UnitWithSuggestion(Unit):
    issues: Optional[List[Dict[str, Any]]] = []

app = FastAPI()

def analyze_and_suggest(code: str) -> Dict:
    suggestions = []
    found_issue = False

    select_full_pattern = re.compile(
        r"(select[\s\S]+?from\s+([a-zA-Z0-9_]+)[\s\S]*?\.)",  # full statement, table
        re.IGNORECASE,
    )

    for m in select_full_pattern.findall(code):
        orig_stmt, main_table = m
        stmt = orig_stmt.strip()
        snippet = stmt
        adjusted_code = stmt
        stmt_lower = stmt.lower()
        issue_msgs = []

        tables_to_replace = {}
        main_table_upper = main_table.upper()
        if main_table_upper in TABLE_MAP:
            new_table = TABLE_MAP[main_table_upper]
            tables_to_replace[main_table_upper] = new_table

        join_pattern = re.compile(r'\bjoin\s+([a-zA-Z0-9_]+)', re.IGNORECASE)
        for jm in join_pattern.findall(stmt):
            join_table_upper = jm.upper()
            if join_table_upper in TABLE_MAP:
                new_j_tbl = TABLE_MAP[join_table_upper]
                tables_to_replace[join_table_upper] = new_j_tbl

        if tables_to_replace:
            for t_old, t_new in tables_to_replace.items():
                issue_msgs.append(
                    f"Use replacement table `{t_new}` instead of `{t_old}`."
                )
                adjusted_code = re.sub(rf'\b{re.escape(t_old)}~', f"{t_new}~", adjusted_code, flags=re.IGNORECASE)
                adjusted_code = re.sub(rf'(\bfrom|\bjoin)\s+{re.escape(t_old)}\b', lambda m: f"{m.group(1)} {t_new}", adjusted_code, flags=re.IGNORECASE)

        # --------- Field detection and SELECT * handling ---------
        sel_fields_match = re.match(r"select\s+(.*?)\s+from", stmt, re.IGNORECASE)
        if sel_fields_match:
            fields = sel_fields_match.group(1).strip()
        else:
            fields = "*"

        adjusted_main_table = tables_to_replace.get(main_table_upper, main_table_upper)
        explicit_fields = fields

        ### FIX: Only use suggested fields if '*' or 'distinct *'
        is_star_select = (fields == "*" or fields.lower() == "distinct *")

        # --- Use SUGGESTED_FIELDS only for * ---
        if is_star_select:
            replacement_fields = (
                ", ".join(SUGGESTED_FIELDS.get(adjusted_main_table, []))
            )
            if replacement_fields:
                issue_msgs.append(f"Avoid `SELECT *`. Use only these fields: {replacement_fields}.")
            else:
                issue_msgs.append("Avoid `SELECT *`. Use an explicit field list.")
                replacement_fields = "<field_list>"
            explicit_fields = replacement_fields
            adjusted_code = re.sub(
                r"select\s+\*\s+from", f"SELECT {replacement_fields} FROM", adjusted_code, flags=re.IGNORECASE
            )
        else:
            ### FIX: If not star select, explicit_fields stays as the fields from SELECT query

            # Explode field list further for later use in ORDER BY/SORT
            field_name_list = [x.strip() for x in fields.replace("distinct","").split(",") if x.strip()]
            explicit_fields = ", ".join(field_name_list)

        # --------- SELECT SINGLE logic ---------
        if fields.strip().lower().startswith("single"):
            issue_msgs.append(
                "Do not use `SELECT SINGLE`. Prefer `SELECT ... UP TO 1 ROWS ORDER BY ... . ENDSELECT.` for clarity and compliance."
            )
            # Remove 'single' and grab actual fields
            real_fields = fields.strip()[len("single "):].strip()
            if not real_fields:
                real_fields = explicit_fields
            is_single_star = (real_fields == "*" or real_fields.lower() == "distinct *")
            if is_single_star:
                replacement_fields = (
                    ", ".join(SUGGESTED_FIELDS.get(adjusted_main_table, []))
                )
                if replacement_fields:
                    real_fields = replacement_fields
                    issue_msgs.append(f"Use only these fields instead of *: {replacement_fields}.")
                else:
                    real_fields = "<field_list>"
            # Here: keep real_fields as actual fields if not '*'
            order_by_clause = f"ORDER BY {real_fields}" if real_fields != "<field_list>" else ""
            where_match = re.search(r"\s+where\s+(.+?)\.?$", stmt, re.IGNORECASE)
            where_clause = f"WHERE {where_match.group(1).strip()}" if where_match else ""
            new_table_str = adjusted_main_table
            adjusted_code = (
                f"SELECT {real_fields} FROM {new_table_str} {where_clause} UP TO 1 ROWS {order_by_clause}. ENDSELECT."
            )
        if fields.strip().lower().startswith("single") and "order by" in stmt_lower:
            issue_msgs.append("Do NOT use `ORDER BY` with `SELECT SINGLE`. Use `UP TO 1 ROWS ... ORDER BY ...` instead.")

        # --------- FOR ALL ENTRIES and ORDER BY logic (ALL improved logic here) ---------
        fae_present = bool(re.search(r"for\s+all\s+entries\s+in\s+", stmt, re.IGNORECASE))
        order_by_match = re.search(r"order\s+by\s+([a-zA-Z0-9_,\s~]+)", stmt, re.IGNORECASE)

        if fae_present:
            if order_by_match:
                order_fields = order_by_match.group(1).strip()
                issue_msgs.append(
                    "When using FOR ALL ENTRIES, do not use `ORDER BY` in SQL. Instead, sort the resulting internal table in ABAP."
                    + (f" Use: SORT <itab> BY {order_fields}." if order_fields else "")
                )
                adjusted_code = re.sub(r"order\s+by\s+([a-zA-Z0-9_,\s~]+)", '', adjusted_code, flags=re.IGNORECASE)
            else:
                # Only suggest SORT if not 'select single'
                if not fields.strip().lower().startswith("single"):
                    # Use SELECT field names, not SUGGESTED_FIELDS, unless is_star_select
                    sort_fields = explicit_fields if not is_star_select else ", ".join(SUGGESTED_FIELDS.get(adjusted_main_table, []))
                    if sort_fields and sort_fields != "<field_list>":
                        issue_msgs.append(f"For deterministic results, sort the resulting internal table in ABAP. Use: SORT <itab> BY {sort_fields}.")
        else:  # Not "FOR ALL ENTRIES"
            if (
                not fields.strip().lower().startswith("single")
                and not order_by_match
            ):
                # Use SELECT field names, not SUGGESTED_FIELDS, unless is_star_select
                order_by_fields = explicit_fields if not is_star_select else ", ".join(SUGGESTED_FIELDS.get(adjusted_main_table, []))
                if order_by_fields and order_by_fields != "<field_list>":
                    issue_msgs.append(f"For deterministic results, add `ORDER BY {order_by_fields}` to the SELECT statement.")
                    adjusted_code = adjusted_code.rstrip('.').strip()
                    adjusted_code += f" ORDER BY {order_by_fields}."

        if issue_msgs:
            found_issue = True
            suggestions.append({
                "suggestion": " ".join(issue_msgs),
                "snippet": snippet,
                "adjusted_code": adjusted_code
            })

    if not found_issue:
        return {}
    return {"issues": suggestions}

@app.post("/analyze", response_model=List[UnitWithSuggestion])
async def analyze_code(units: List[Unit]) -> List[UnitWithSuggestion]:
    results = []
    for unit in units:
        res = analyze_and_suggest(unit.code or "")
        issues = res.get("issues", [])
        # preserve all fields, append issues
        result = unit.dict()
        result['issues'] = issues
        results.append(result)
    return results

# ========== Start with: uvicorn main:app --reload ===========
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)