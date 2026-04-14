# 🚀 AWS Data Engineering Labs — ETL vs ELT

## 📌 Overview

This repository contains **hands-on labs** to understand how modern data pipelines work using AWS.

You will explore:

* Traditional **ETL (Extract, Transform, Load)**
* Modern **ELT (Extract, Load, Transform)**
* The role of a **data lake** in both approaches

---

## 🎯 Learning Objectives

By completing these labs, you will be able to:

* Understand the difference between ETL and ELT
* Work with core AWS data services
* Build simple data pipelines
* Query and transform raw data
* Compare modern vs traditional architectures

---

## 🧱 Architecture Diagram

```
+----------------------+
|     Data Sources     |
|----------------------|
|  CSV / Logs / API    |
+----------+-----------+
           |
           v
+----------------------+
|      Amazon S3       |
|  (Data Lake - RAW)   |
+----------+-----------+
           |
    -------------------------
    |                       |
    v                       v

+----------------------+   +------------------------------+
|    ETL (AWS Glue)    |   |   ELT (Athena / Redshift)    |
| Transform BEFORE     |   | Transform AFTER loading      |
| loading              |   | (SQL inside warehouse)       |
+----------+-----------+   +-------------+----------------+
           |                             |
           v                             v

+----------------------+   +------------------------------+
|   Processed Data     |   |   Transformed Data (SQL)     |
|   (Cleaned in S3)    |   |   (Views / Tables)           |
+----------+-----------+   +-------------+----------------+
           |                             |
           -------------+----------------
                        |
                        v

             +--------------------------+
             |   Analytics / BI / ML    |
             | Dashboards & Insights    |
             +--------------------------+
```

---

## 📂 Repository Structure

```
aws-data-engineering-labs-etl-elt/
│
├── data/
│   ├── raw/              # Raw input data
│   └── processed/        # Cleaned data (ETL output)
│
├── demo1-data-lake-athena/
│   └── README.md         # Query raw data (ELT style)
│
├── demo2-elt-redshift/
│   └── transform.sql     # ELT transformations
│
├── demo3-etl-glue/
│   └── etl_script.py     # ETL pipeline script
│
└── README.md
```

---

## 🧪 Demos

### 🥇 Demo 1 — Data Lake + Query Raw Data

**Tools:** Amazon S3 + Amazon Athena

* Store raw data in a data lake
* Query data **without preprocessing**
* Apply transformations using SQL

👉 Shows how **ELT works in practice**

---

### 🥈 Demo 2 — ELT in Data Warehouse

**Tool:** Amazon Redshift

* Load raw data into warehouse
* Transform using SQL
* Create analytics-ready tables

👉 Shows **modern cloud data workflows**

---

### 🥉 Demo 3 — ETL Pipeline

**Tool:** AWS Glue

* Clean and transform data before loading
* Store processed data in S3
* Structured pipeline approach

👉 Shows **traditional ETL**

---

## ▶️ How to Use This Repo

1. Upload data from `/data/raw/` to S3

2. Follow demos in order:

   👉 **Recommended order:**

   ```
   Demo 1 → Demo 2 → Demo 3
   ```

3. Run queries / scripts in each demo folder

---

## ⚖️ ETL vs ELT Summary

| Feature        | ETL                  | ELT                        |
| -------------- | -------------------- | -------------------------- |
| When transform | Before loading       | After loading              |
| Flexibility    | Low                  | High                       |
| Speed          | Slower ingestion     | Faster ingestion           |
| Use case       | Structured pipelines | Big data & cloud analytics |

---

## 💡 Key Takeaways

* A **data lake** stores raw data cheaply and at scale
* **ETL** is structured but less flexible
* **ELT** is modern, faster, and more scalable
* AWS enables both approaches easily

---

## 📚 Technologies Used

* Amazon S3
* Amazon Athena
* Amazon Redshift
* AWS Glue

---