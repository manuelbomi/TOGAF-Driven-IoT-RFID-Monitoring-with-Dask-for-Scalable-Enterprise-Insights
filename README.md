# TOGAF-Driven Enterprise IoT & RFID Analytics: High-Performance Dask Implementation

## Overview

This repository demonstrates an enterprise-grade approach to **IoT and RFID analytics**, leveraging **TOGAF principles** and **high-performance distributed processing with Dask**. The goal is to monitor, analyze, and derive actionable insights from large-scale sensor and RFID datasets while maintaining enterprise scalability and operational intelligence.

Two complementary projects are integrated here:

1. **Enterprise-IoT-Dask** – Temperature monitoring, cold chain compliance, and sensor anomaly detection.
2. **Dask-Based-Distributed-IoT-RFID-Enterprise-Asset-Monitoring-and-Tracking** – RFID asset tracking, dwell time analysis, and gateway performance monitoring.

More detailed overviews of both projects are availanle here: *https://github.com/manuelbomi/Enterprise-IoT-Dask---High-Performance-Distributed-Analytics-for-RFID-IoT-Sensors-Data*     ; and here: *https://github.com/manuelbomi/Dask-Based-Distributed-IoT-RFID-Enterprise-Asset-Monitoring-and-Tracking*

#### Together, these projects demonstrate a **TOGAF-aligned incremental implementation** for enterprise-scale analytics.

---

## TOGAF Architecture Overview

TOGAF (The Open Group Architecture Framework) provides a structure for **designing, planning, and governing enterprise IT architectures**. In this context:

- **Business Architecture:** Cold chain compliance, asset visibility, operational intelligence
- **Data Architecture:** Sensor, RFID, and gateway data pipelines
- **Application Architecture:** Analytics scripts, dashboards, and alert systems
- **Technology Architecture:** High-performance computing using **Dask**, distributed storage, and visualization tools

**Incremental Implementation Approach:**

1. **Phase 1:** Data ingestion & basic analytics with Pandas  
2. **Phase 2:** Distributed analytics & scalability with Dask  
3. **Phase 3:** Real-time operational intelligence, dashboards, and alerts  

---

## Phase 1: Prototyping with Pandas

### Temperature Monitoring Example

```python
import pandas as pd

df_temp = pd.read_json('temperature_logs.json', lines=True)
df_temp['timestamp'] = pd.to_datetime(df_temp['timestamp'])

facility_summary = df_temp.groupby('facility').agg({
    'temperature': ['count', 'mean', 'std', 'min', 'max'],
    'epc': 'nunique'
}).round(2)
facility_summary.columns = ['Record_Count', 'Mean_Temp', 'Std_Temp', 'Min_Temp', 'Max_Temp', 'Unique_Sensors']
facility_summary['Temp_Variance'] = (facility_summary['Std_Temp'] ** 2).round(2)
print(facility_summary)
```

#### RFID Asset Tracking Example
```python
df_rfid = pd.read_json('rfid_events.json', lines=True)
df_flat = pd.json_normalize(df_rfid['tagInventoryEvent'])
df_flat['timestamp'] = pd.to_datetime(df_flat['timestamp'])

movement_analysis = (
    df_flat.groupby('epc_number')
    .agg({
        'gateway_location': ['first', 'last', 'nunique'],
        'timestamp': ['min', 'max']
    })
)
movement_analysis.columns = ['first_location', 'last_location', 'locations_visited', 'first_seen', 'last_seen']
print(movement_analysis.head())
```

## Phase 2: Distributed Analytics with Dask

### Temperature Monitoring with Dask

```python
import dask.dataframe as dd

df_temp_dask = dd.read_json('temperature_logs.json', lines=True)
df_temp_dask['timestamp'] = dd.to_datetime(df_temp_dask['timestamp'])

def check_thresholds(partition):
    partition['threshold_breach'] = partition['temperature'] > 25
    return partition

df_monitored = df_temp_dask.map_partitions(check_thresholds)
df_results = df_monitored.groupby('facility').agg({'threshold_breach': 'mean'}).compute()
print(df_results)
```

### RFID Asset Tracking & Dwell Times

```python
df_rfid_dask = dd.read_json('rfid_events.json', lines=True)

def calculate_dwell_times(partition):
    partition = partition.sort_values(['epc_number', 'timestamp'])
    partition['next_ts'] = partition.groupby('epc_number')['timestamp'].shift(-1)
    partition['dwell_sec'] = (partition['next_ts'] - partition['timestamp']).dt.total_seconds()
    return partition

df_dwell = df_rfid_dask.map_partitions(calculate_dwell_times)
dwell_summary = df_dwell.groupby('gateway_location')['dwell_sec'].mean().compute()
print(dwell_summary)


```

## Phase 3: Real-Time Operational Intelligence

### Example: Asset Distribution Visualization

```python
import matplotlib.pyplot as plt

location_distribution = df_dwell.groupby(['gateway_location', 'asset_type']).agg({'epc_number': 'count'}).compute()
location_distribution.unstack().plot(kind='bar', stacked=True)
plt.title("Current Asset Distribution")
plt.show()
```



