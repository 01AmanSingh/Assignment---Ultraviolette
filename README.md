# Assignment---Ultraviolette

1. Assumptions about the signals

I consider each row in the dataset to be a snapshot of a single vehicle at a specific point in time.
Speed is interpreted in kilometers per hour, temperatures in degrees Celsius, battery voltage in volts, battery current in amperes, and state of charge as a percentage between 0 and 100.

In real production environments, telematics data is rarely perfect. Sensors can occasionally produce noisy or incorrect values, and brief data dropouts are common. Because of this, I designed the pipeline so that one faulty sensor reading does not automatically invalidate an entire row if the remaining fields are usable.

Trips are treated as logical driving sessions identified by trip_id, and each trip can contain many timestamped records.

I also assume that when consecutive records are separated by more than five minutes (300 seconds), it most likely represents a communication gap or missing data rather than a normal driving pause.




2. Validation rules used

Each row is checked independently using a two-level validation approach.

Level 1 – Hard Rejection
A row is immediately rejected if:

trip_id is missing

timestamp cannot be parsed into a valid UTC datetime

Such rows cannot be placed correctly on a timeline, so they are written to the rejected dataset along with a clear reason.

Level 2 – Partial Salvage
All numeric fields are converted from strings to numbers where possible.
Sensor values are validated using realistic physical ranges:

Speed: 0–250 km/h

Battery voltage: 200–1000 V

Battery current: -500 to 500 A

SOC: 0–100 %

Motor temperature: -40 to 200 °C

Cell temperature: -40 to 100 °C

If a value falls outside its allowed range, only that specific field is set to null and documented in salvage_notes.
If all sensor fields in a row are invalid, the row is rejected.

This strategy allows me to retain as much useful information as possible while still protecting data quality.



3. Handling duplicates and ordering

Telematics records can arrive out of order due to network delays.

Before performing any time-based calculations, all records are:

Grouped by trip_id

Sorted by timestamp

This guarantees that calculations are always based on the correct chronological order.

When duplicate timestamps exist, the time difference between them becomes zero during the diff() calculation. Since duration and distance are derived from time deltas, these duplicates do not distort results and therefore do not require explicit removal.



4. Why temporary data structures exist

Two explicit intermediate datasets are created:

Cleaned dataset
Contains validated, typed, and partially salvaged rows used for analytics.

Rejected dataset
Contains rows that failed validation with clear rejection reasons.

These datasets provide:

Auditability (why rows were rejected)

Debugging capability

Transparency into data quality

Separation between ingestion and aggregation

This mirrors real production pipelines where raw, cleaned, and rejected data are stored independently.



5. Time-series processing approach

Once records are ordered by timestamp within each trip:

Time differences between consecutive records are calculated

Gaps are flagged when the difference exceeds 300 seconds

Trip duration is obtained by summing time deltas

Distance is estimated using speed multiplied by elapsed time

Because these computations depend on timestamp ordering, they demonstrate true time-series-aware processing.





