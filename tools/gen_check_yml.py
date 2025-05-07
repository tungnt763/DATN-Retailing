import json
import os
import yaml

# Load metadata JSON
with open("/Users/tungnt763/Documents/DATN-Retailing/dags/config/load_layer_table_info.json") as f:
    metadata = json.load(f)

# Custom string format for multiline SQL
class LiteralStr(str): pass
def literal_str_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
yaml.add_representer(LiteralStr, literal_str_representer)

# Soda-supported formats (from docs)
SODA_FORMATS = {
    "credit card number", "date eu", "date inverse", "date iso 8601", "date us",
    "decimal", "decimal comma", "decimal point", "email", "integer", "ip address",
    "ipv4 address", "ipv6 address", "money", "money comma", "money point",
    "negative decimal", "negative decimal comma", "negative decimal point",
    "negative integer", "negative percentage", "negative percentage comma",
    "negative percentage point", "percentage", "percentage comma", "percentage point",
    "phone number", "positive decimal", "positive decimal comma", "positive decimal point",
    "positive integer", "positive percentage", "positive percentage comma",
    "positive percentage point", "time 12h", "time 12h nosec", "time 24h", "time 24h nosec",
    "timestamp 12h", "timestamp 24h", "uuid"
}

def generate_check_block(table_name, table_info):
    checks = []
    required_columns = []
    column_types = {}

    # Gather schema info
    for col in sorted(table_info["columns"], key=lambda x: int(x["index"])):
        column_types[col["physical_name"]] = col["type"].lower()
        if col["mode"] == "REQUIRED":
            required_columns.append(col["physical_name"])

    # 1. Schema check
    checks.append({
        "schema": {
            "fail": {
                "when required column missing": required_columns,
                "when wrong column type": column_types
            }
        }
    })

    # 2. Row count
    checks.append({})
    checks.append({
        "row_count > 0": {
            "name": f"{table_name.capitalize()} table contains data"
        }
    })

    # 3. Column count check
    fail_query = LiteralStr(
        f"SELECT COUNT(*) AS col_count\n"
        f"FROM `datn-retailing.edw_loaded.INFORMATION_SCHEMA.COLUMNS`\n"
        f"WHERE table_name = '{table_name}_temp'\n"
        f"HAVING col_count != {table_info['col_nums']}"
    )
    checks.append({})
    checks.append({
        "failed rows": {
            "name": f"Column count must be {table_info['col_nums']}",
            "fail query": fail_query
        }
    })

    # 4. Required non-null fields
    for col in table_info["columns"]:
        if col["mode"] == "REQUIRED":
            checks.append({})
            checks.append({
                f"missing_count({col['physical_name']}) = 0": {
                    "name": f"{col['logical_name_en']} is required"
                }
            })

    # 5. Format checks (only valid if in SODA_FORMATS)
    for col in table_info["columns"]:
        fmt = col.get("format", "").strip().lower()
        if fmt in SODA_FORMATS:
            checks.append({})
            checks.append({
                f"invalid_count({col['physical_name']}) = 0": {
                    "name": f"{col['logical_name_en']} format validation",
                    "valid format": fmt
                }
            })

    # 6. Regex checks
    for col in table_info["columns"]:
        if col.get("regax"):
            checks.append({})
            checks.append({
                f"invalid_count({col['physical_name']}) = 0": {
                    "name": f"{col['logical_name_en']} regex validation",
                    "valid regex": col["regax"]
                }
            })

    # 7. Valid values
    for col in table_info["columns"]:
        values = col.get("values")
        if isinstance(values, list) and values:
            flat_values = "[" + ", ".join(f'"{v}"' for v in values) + "]"
            checks.append({})
            checks.append({
                f"invalid_percentage({col['physical_name']}) = 0": {
                    "name": f"{col['logical_name_en']} must be in allowed values",
                    "valid values": LiteralStr(flat_values)
                }
            })

    # 8. Custom SQL queries
    for col in table_info["columns"]:
        if col.get("other"):
            for line in col["other"].splitlines():
                line = line.strip()
                if line:
                    checks.append({})
                    checks.append({
                        "failed rows": {
                            "name": f"Custom check on {col['physical_name']}",
                            "fail query": LiteralStr(line)
                        }
                    })

    # Clean up: remove any leading/trailing blank blocks
    filtered = [check for i, check in enumerate(checks) if i == 0 or check]
    return {f"checks for {table_name}_temp": filtered}

# Output folder
output_dir = "/Users/tungnt763/Documents/DATN-Retailing/include/soda/edw_loaded/checks/sources"
os.makedirs(output_dir, exist_ok=True)

# Generate all YAML files
for table_name, table_info in metadata.items():
    data = generate_check_block(table_name, table_info)
    output_path = os.path.join(output_dir, f"check_{table_name}.yml")
    with open(output_path, "w") as f:
        yaml.dump(data, f, sort_keys=False, default_flow_style=False)

print("✅ Generated Soda check YAML files")
