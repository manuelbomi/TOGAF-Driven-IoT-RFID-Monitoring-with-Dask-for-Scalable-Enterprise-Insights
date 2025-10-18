# TOGAF-Driven Enterprise IoT & RFID Analytics: High-Performance Dask Implementation

## Overview

This repository demonstrates an enterprise-grade approach to **IoT and RFID analytics**, leveraging **TOGAF principles** and **high-performance distributed processing with Dask**. The goal is to monitor, analyze, and derive actionable insights from large-scale sensor and RFID datasets while maintaining enterprise scalability and operational intelligence.

Two complementary projects are integrated here:

1. **Enterprise-IoT-Dask** – Temperature monitoring, cold chain compliance, and sensor anomaly detection.
2. **Dask-Based-Distributed-IoT-RFID-Enterprise-Asset-Monitoring-and-Tracking** – RFID asset tracking, dwell time analysis, and gateway performance monitoring.

#### More detailed overviews of both projects are availanle here: 

<ins>Repository 1</ins>: *https://github.com/manuelbomi/Enterprise-IoT-Dask---High-Performance-Distributed-Analytics-for-RFID-IoT-Sensors-Data*     ; 

#### and here: 

<ins>Repository 2</ins>: *https://github.com/manuelbomi/Dask-Based-Distributed-IoT-RFID-Enterprise-Asset-Monitoring-and-Tracking*


#### Together, these projects demonstrate a **TOGAF-aligned incremental implementation** for enterprise-scale analytics.

---

## TOGAF Architecture Overview

TOGAF (The Open Group Architecture Framework) provides a structure for **designing, planning, and governing enterprise IT architectures**. In this context:

- **Business Architecture:** Cold chain compliance, asset visibility, operational intelligence
- **Data Architecture:** Sensor, RFID, and gateway data pipelines
- **Application Architecture:** Analytics scripts, dashboards, and alert systems
- **Technology Architecture:** High-performance computing using **Dask**, distributed storage, and visualization tools

#### Visualizing TOGAF IoT/RFID Analytics Architecture

```python

+-------------------+      +--------------------+      +--------------------+
| IoT / RFID Sensors| ---> | Data Ingestion     | ---> | Distributed Dask   |
| Temperature/Asset |      | (JSON, CSV streams)|      | Analytics Cluster  |
+-------------------+      +--------------------+      +--------------------+
                                    |
                                    v
                             +------------------+
                             | Pandas Prototype |
                             | (Small Scale)    |
                             +------------------+
                                    |
                                    v
                             +------------------+
                             | Real-Time Dashboards|
                             | & Alerts           |
                             +------------------+



```

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

- Generate dashboards and alerts for temperature breaches or unusual asset movement.

- Integrate operational intelligence directly into enterprise workflows.

---

### Incremental TOGAF Roadmap

| TOGAF Phase              | Implementation Step                                        | Repository Reference              |
|--------------------------|------------------------------------------------------------|----------------------------------|
| Architecture Vision      | Identify key IoT/RFID sensors & facilities                | Enterprise-IoT-Dask              |
| Business Architecture    | Define compliance, asset tracking, operational goals     | Both repositories                |
| Data Architecture        | Generate and standardize temperature & RFID datasets     | Both repositories                |
| Application Architecture | Implement analytics scripts, dashboards                   | Both repositories                |
| Technology Architecture  | Introduce Dask clusters for HPC & parallel processing    | Both repositories                |
| Implementation Governance| Validate metrics, monitor pipelines, generate alerts    | Both repositories                |

---

## Roadmaps for TOAGF Step 4 - Step 8

#### This roadmap builds upon the initial steps (Phases 1–3) where key sensors, compliance metrics, and datasets were defined. It shows a progressive approach for enterprises to incrementally implement, scale, and maintain high-performance IoT and RFID analytics using Dask. Code snippets such as generating RFID events, computing rolling statistics, and setting up Dask clusters from both repositories can be directly referenced in Phase D–H implementations.

```python
| TOGAF Phase                                 | Implementation Step                                                                                                                               | Repository Reference                                                     |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| **Phase D: Technology Architecture**        | Deploy Dask clusters and HPC infrastructure to support parallelized IoT/RFID analytics, containerize services with Docker for reproducibility     | Both repositories                                                        |
| **Phase E: Opportunities & Solutions**      | Integrate IoT and RFID analytics pipelines with dashboards (Streamlit, Matplotlib/Seaborn) and define incremental deployment milestones           | Dask-Based-Distributed-IoT-RFID-Enterprise-Asset-Monitoring-and-Tracking |
| **Phase F: Migration Planning**             | Develop deployment scripts for rolling out analytics to production sensors and facilities; plan incremental scaling of HPC resources              | Both repositories                                                        |
| **Phase G: Implementation Governance**      | Validate analytics results, monitor performance, track anomalies and compliance metrics; generate real-time alerts                                | Both repositories                                                        |
| **Phase H: Architecture Change Management** | Continuously adapt pipelines to new sensor types, larger datasets, and evolving business requirements; incorporate feedback into the architecture | Both repositories                                                        |


```



---

## Key Enterprise Benefits

- Scalable Analytics: Millions of sensor & RFID events processed in parallel

- Operational Intelligence: Real-time dashboards & alerts

- Predictive Compliance: Early detection of temperature breaches or asset anomalies

- Low Memory Footprint: Dask avoids Pandas memory bottlenecks

- Incremental Implementation: Supports TOGAF ADM cycles for enterprise adoption


## Acknowledgements

### Dask
 - for distributed computing

### TOGAF
 - for enterprise architecture guidance

- Original repositories:

Enterprise-IoT-Dask

Dask-Based-Distributed-IoT-RFID-Enterprise-Asset-Monitoring-and-Tracking




