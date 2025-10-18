# TOGAF-Driven Enterprise IoT & RFID Analytics: High-Performance Dask Implementation

## Overview

This repository demonstrates an enterprise-grade approach to **IoT and RFID analytics**, leveraging **TOGAF principles** and **high-performance distributed processing with Dask**. The goal is to monitor, analyze, and derive actionable insights from large-scale sensor and RFID datasets while maintaining enterprise scalability and operational intelligence.

Two complementary projects are integrated here:

1. **Enterprise-IoT-Dask** – Temperature monitoring, cold chain compliance, and sensor anomaly detection.
2. **Dask-Based-Distributed-IoT-RFID-Enterprise-Asset-Monitoring-and-Tracking** – RFID asset tracking, dwell time analysis, and gateway performance monitoring.

Together, these projects demonstrate a **TOGAF-aligned incremental implementation** for enterprise-scale analytics.

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

