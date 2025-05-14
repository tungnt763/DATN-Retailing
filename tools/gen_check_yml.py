import json
import os
import yaml

# Configuration
DATASET = "edw"
METADATA = "edw_layer_table_info"
METADATA_PATH = f"/Users/tungnt763/Documents/DATN-Retailing/dags/config/{METADATA}.json"
OUTPUT_DIR = f"/Users/tungnt763/Documents/DATN-Retailing/include/soda/{DATASET}/checks/sources"
TABLE_NAME_SUFFIX = "_temp" if DATASET == "edw_loaded" else ""

# Load metadata
with open(METADATA_PATH) as f:
    metadata = json.load(f)

# Pretty YAML for SQL blocks
class LiteralStr(str): pass
def literal_str_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
yaml.add_representer(LiteralStr, literal_str_representer)

# Supported formats
SODA_FORMATS = {
    "email", "date inverse", "positive integer", "positive decimal point", "phone number", "decimal point"
}

def generate_checks(table_name, table_info):
    checks = {f"checks for {table_name}{TABLE_NAME_SUFFIX}": []}
    required_cols = []
    pk_cols = []
    column_types = {}

    # Prepare schema info
    for col in sorted(table_info["columns"], key=lambda c: int(c["index"])):
        column_types[col["physical_name"]] = col["type"].lower()
        if col["mode"] == "REQUIRED":
            required_cols.append(col["physical_name"])
        if col["pk"] == "Y":
            pk_cols.append(col["physical_name"])

    # 1. Schema checks
    checks[f"checks for {table_name}{TABLE_NAME_SUFFIX}"].append({
        "schema": {
            "fail": {
                "when required column missing": required_cols,
                "when wrong column type": column_types
            }
        }
    })

    # 2. Row count must be positive
    checks[f"checks for {table_name}{TABLE_NAME_SUFFIX}"].append({
        "row_count > 0": {
            "name": f"{table_name} should contain data"
        }
    })

    if DATASET == "edw_loaded":
        # 3. Column count exact match
        col_count_check = LiteralStr(
            f"SELECT COUNT(*) AS col_count\n"
            f"FROM `datn-retailing.{DATASET}.INFORMATION_SCHEMA.COLUMNS`\n"
            f"WHERE table_name = '{table_name}{TABLE_NAME_SUFFIX}'\n"
            f"HAVING col_count != {int(table_info['col_nums']) + 6}"
        )
        checks[f"checks for {table_name}{TABLE_NAME_SUFFIX}"].append({
            "failed rows": {
                "name": f"Column count should be exactly {int(table_info['col_nums']) + 6}",
                "fail query": col_count_check
            }
        })

    # 3. Unique
    if DATASET != "edw_loaded" and pk_cols:
        checks[f"checks for {table_name}{TABLE_NAME_SUFFIX}"].append({
            f"duplicate_count({', '.join(pk_cols)}) = 0": {
                "name": f"({', '.join(pk_cols)}) must be unique"
            }
        })

    # 4. Per-column checks
    for col in table_info["columns"]:
        name = col["physical_name"]
        logic_name = col.get("logical_name_en", name)

        # Completeness
        if col["mode"] == "REQUIRED":
            checks[f"checks for {table_name}{TABLE_NAME_SUFFIX}"].append({
                f"missing_count({name}) = 0": {
                    "name": f"{logic_name} must not be null"
                }
            })

        # Format validity
        fmt = col.get("format", "").strip().lower()
        if fmt in SODA_FORMATS and col.get("type") == "STRING" and DATASET != "edw":
            checks[f"checks for {table_name}{TABLE_NAME_SUFFIX}"].append({
                f"invalid_count({name}) = 0": {
                    "name": f"{logic_name} must match format",
                    "valid format": fmt
                }
            })

        # Regex validity
        if col.get("regax") and col.get("type") == "STRING" and DATASET != "edw":
            checks[f"checks for {table_name}{TABLE_NAME_SUFFIX}"].append({
                f"invalid_count({name}) = 0": {
                    "name": f"{logic_name} regex validation",
                    "valid regex": col["regax"]
                }
            })

        # Enum values validity
        if isinstance(col.get("values"), list) and col["values"] and DATASET != "edw":
            allowed = LiteralStr("\n".join(f"- {v}" for v in col["values"]))
            checks[f"checks for {table_name}{TABLE_NAME_SUFFIX}"].append({
                f"invalid_percent({name}) = 0": {
                    "name": f"{logic_name} must be within allowed values",
                    "valid values": allowed
                }
            })

        # Custom fail SQL
        if col.get("other"):
            for line in col["other"].splitlines():
                if line.strip():
                    checks[f"checks for {table_name}{TABLE_NAME_SUFFIX}"].append({
                        "failed rows": {
                            "name": f"Custom check on {name}",
                            "fail query": LiteralStr(line.strip())
                        }
                    })

    return checks

# Ensure output folder exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Generate YAML files
for table_name, table_info in metadata.items():
    checks = generate_checks(table_info["physical_name"], table_info)
    with open(os.path.join(OUTPUT_DIR, f"check_{table_name}.yml"), "w") as f:
        yaml.dump(checks, f, sort_keys=False, default_flow_style=False)

print("✅ Soda check YAML files generated.")
