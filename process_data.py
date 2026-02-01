# processing.py (robust + schema-aware version)

import pandas as pd
import numpy as np
import os

# ---------------------------------
# Configuration
# ---------------------------------
CSV_PATH = "vehicle_telematics.csv"
CHUNK_SIZE = 100_000
GAP_THRESHOLD_SEC = 300  # 5 minutes
OUTPUT_DIR = "output"

REQUIRED_COLUMNS = {
    "trip_id",
    "timestamp",
    "speed_kmph",
    "battery_voltage",
    "battery_current",
    "soc_percent",
    "motor_temp_c",
    "cell_temp_c",
}

VALID_RANGES = {
    "speed_kmph": (0, 250),
    "battery_voltage": (200, 1000),
    "battery_current": (-500, 500),
    "soc_percent": (0, 100),
    "motor_temp_c": (-40, 200),
    "cell_temp_c": (-40, 100),
}

COLUMN_ALIASES = {
    "trip_id": ["trip_id", "tripid", "vehicle_id", "vehicleid"],
    "timestamp": ["timestamp", "time", "utc_timestamp", "datetime"],
    "speed_kmph": ["speed_kmph", "speed", "vehicle_speed"],
    "battery_voltage": ["battery_voltage", "batteryvoltage", "voltage"],
    "battery_current": ["battery_current", "batterycurrent", "current"],
    "soc_percent": ["soc_percent", "soc", "state_of_charge"],
    "motor_temp_c": ["motor_temp_c", "motortemp", "motor_temperature"],
    "cell_temp_c": ["cell_temp_c", "celltemp", "cell_temperature"],
}

# ---------------------------------
# Helpers
# ---------------------------------
def normalize_columns(df):
    df.columns = df.columns.str.strip().str.lower()
    return df

def resolve_columns(df):
    rename_map = {}
    for canonical, aliases in COLUMN_ALIASES.items():
        for col in df.columns:
            if col in aliases:
                rename_map[col] = canonical
                break
    return df.rename(columns=rename_map)

def coerce_float(series):
    return pd.to_numeric(series, errors="coerce")

def is_trip_level_schema(df):
    """
    Detect already-aggregated trip-level data.
    """
    trip_level_signals = {
        "duration_minutes",
        "distance_km",
        "speed_avg",
        "speed_max",
        "energy_consumed_kwh",
    }
    return bool(trip_level_signals.intersection(set(df.columns)))

# ---------------------------------
# Sanity check CSV
# ---------------------------------
if not os.path.exists(CSV_PATH):
    raise FileNotFoundError(f"Input CSV not found: {CSV_PATH}")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------
# Read first chunk to detect schema
# ---------------------------------
reader = pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE)
first_chunk = next(reader)
first_chunk = normalize_columns(first_chunk)

IS_TRIP_LEVEL = is_trip_level_schema(first_chunk)

# ---------------------------------
# TRIP-LEVEL DATA PATH (FIX)
# ---------------------------------
if IS_TRIP_LEVEL:
    df = first_chunk
    for chunk in reader:
        chunk = normalize_columns(chunk)
        df = pd.concat([df, chunk], ignore_index=True)

    df = resolve_columns(df)

    df = df[df["trip_id"].notna()]

    trip_metrics_df = pd.DataFrame({
        "trip_id": df["trip_id"],
        "duration_minutes": df.get("duration_minutes"),
        "avg_speed": df.get("speed_avg"),
        "distance_km": df.get("distance_km"),
        "max_speed": df.get("speed_max"),
        "max_motor_temp": df.get("motor_temp_max"),
        "max_cell_temp": df.get("cell_temp_max"),
        "energy_consumed_kwh": df.get("energy_consumed_kwh"),
    })

    trip_metrics_df.to_csv(f"{OUTPUT_DIR}/trip_metrics.csv", index=False)
    df.to_csv(f"{OUTPUT_DIR}/cleaned_telematics.csv", index=False)

    print("=== Processing Complete ===")
    print("Trip-level data detected")
    print(f"Trips processed      : {len(trip_metrics_df)}")
    print(f"Output directory     : {OUTPUT_DIR}/")
    exit(0)

# ---------------------------------
# TIME-SERIES DATA PATH (ORIGINAL)
# ---------------------------------
validated_rows = []
rejected_rows = []
total_rows_read = 0

chunks = [first_chunk] + list(reader)

for chunk in chunks:
    total_rows_read += len(chunk)

    chunk = normalize_columns(chunk)
    chunk = resolve_columns(chunk)

    # Ensure all required columns exist
    for col in REQUIRED_COLUMNS:
        if col not in chunk.columns:
            chunk[col] = np.nan

    chunk["rejection_reasons"] = ""
    chunk["salvage_notes"] = ""
    chunk["validation_status"] = "valid"

    # Timestamp parsing
    chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors="coerce", utc=True)

    # Numeric coercion
    for col in VALID_RANGES:
        chunk[col] = coerce_float(chunk[col])

    # Hard rejects
    chunk.loc[chunk["trip_id"].isna(), "rejection_reasons"] += "|missing_trip_id"
    chunk.loc[chunk["timestamp"].isna(), "rejection_reasons"] += "|invalid_timestamp"

    # Sensor validation (partial salvage)
    for col, (lo, hi) in VALID_RANGES.items():
        invalid = ~chunk[col].between(lo, hi)
        chunk.loc[invalid, col] = np.nan
        chunk.loc[invalid, "salvage_notes"] += f"|{col}_out_of_range"

    # Fully invalid sensor rows
    all_nan = chunk[list(VALID_RANGES.keys())].isna().all(axis=1)
    chunk.loc[all_nan, "rejection_reasons"] += "|all_sensors_invalid"

    # Final classification
    rejected_mask = chunk["rejection_reasons"] != ""
    chunk.loc[rejected_mask, "validation_status"] = "rejected"

    validated_rows.append(chunk[~rejected_mask])
    rejected_rows.append(chunk[rejected_mask])

# ---------------------------------
# Explicit Intermediate Datasets
# ---------------------------------
cleaned_df = pd.concat(validated_rows, ignore_index=True) if validated_rows else pd.DataFrame()
rejected_df = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()

# ---------------------------------
# Time-Series Processing
# ---------------------------------
metrics = []

if not cleaned_df.empty:
    for trip_id, g in cleaned_df.groupby("trip_id"):
        g = g.sort_values("timestamp")

        g["dt_sec"] = g["timestamp"].diff().dt.total_seconds().clip(lower=0)
        g["is_gap"] = g["dt_sec"] > GAP_THRESHOLD_SEC

        duration_sec = g["dt_sec"].sum()
        distance_km = (g["speed_kmph"].fillna(0) * g["dt_sec"].fillna(0) / 3600).sum()

        metrics.append({
            "trip_id": trip_id,
            "rows": len(g),
            "duration_minutes": duration_sec / 60,
            "gap_count": int(g["is_gap"].sum()),
            "max_speed": g["speed_kmph"].max(skipna=True),
            "avg_speed": g["speed_kmph"].mean(skipna=True),
            "distance_km": distance_km,
            "max_motor_temp": g["motor_temp_c"].max(skipna=True),
            "max_cell_temp": g["cell_temp_c"].max(skipna=True),
            "min_battery_voltage": g["battery_voltage"].min(skipna=True),
            "max_current": g["battery_current"].max(skipna=True),
        })

final_metrics_df = pd.DataFrame(metrics)

# ---------------------------------
# Persist Outputs (ALWAYS)
# ---------------------------------
cleaned_df.to_csv(f"{OUTPUT_DIR}/cleaned_telematics.csv", index=False)
rejected_df.to_csv(f"{OUTPUT_DIR}/rejected_telematics.csv", index=False)
final_metrics_df.to_csv(f"{OUTPUT_DIR}/trip_metrics.csv", index=False)

print("=== Processing Complete ===")
print(f"Total rows read     : {total_rows_read}")
print(f"Valid rows          : {len(cleaned_df)}")
print(f"Rejected rows       : {len(rejected_df)}")
print(f"Output directory    : {OUTPUT_DIR}/")


