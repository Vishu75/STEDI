# STEDI Human Balance Analytics

This project demonstrates how to design a **data lakehouse architecture** for the STEDI team, where sensor and mobile app data are transformed into a dataset suitable for training a machine learning model.

---

## 📌 Project Overview
The **STEDI Step Trainer** is a hardware device designed to help users practice balance exercises. It:  

- Coaches users through a balance routine  
- Includes sensors that record step-related motion data  
- Connects to a mobile application that collects customer information and accelerometer readings from smartphones  

Customers who received the Step Trainer have begun using it with the app, generating both **sensor readings** and **accelerometer signals**.  

- The Step Trainer captures **distances from nearby objects**  
- The app tracks **movement along the X, Y, and Z axes**  

The STEDI team’s objective is to use this data to train a **real-time step detection algorithm**.  
⚠️ Only customers who **explicitly agreed** to share their data for research are included in the training dataset.  

---

## 🛠️ Solution Summary
As the data engineer for the STEDI project, the task is to **ingest, process, and curate** the Step Trainer and app data into a **structured data lakehouse on AWS**, making it ready for data scientists to train the model.  

The pipeline uses:  
- **Python & Apache Spark**  
- **AWS Glue** (for ETL)  
- **Amazon Athena** (for querying)  
- **Amazon S3** (for storage)  

---

## 📂 Data Sources
### 1. Customer Data (Website/Fulfillment)
Fields include:  
- `serialnumber`  
- `birthday`  
- `registrationdate`  
- `customername`  
- `email`  
- `phone`  
- `lastupdatedate`  
- `sharewithpublicasofdate`  
- `sharewithfriendsasofdate`  
- `sharewithresearchasofdate`  

### 2. Step Trainer Data (Device Sensors)
- `sensorReadingTime`  
- `serialNumber`  
- `distanceFromObject`  

### 3. Accelerometer Data (Mobile App)
- `timeStamp`  
- `user`  
- `x`, `y`, `z` (movement readings)  

---

## 🔄 Architecture & Processing Flow
The solution is built on **Amazon S3**, with distinct zones for each stage of processing:  

### 1️⃣ Landing Zone – Raw Data Intake
- Raw **sensor, app, and customer data** arrive in S3 landing buckets  
- Glue crawlers and Athena tables are created over these datasets for **initial exploration and validation**  

### 2️⃣ Trusted Zone – Consent Filtering
- Customer records are filtered so that only those who have **agreed to share data for research** are retained  
- Accelerometer records are filtered the same way, ensuring only consenting customer readings are stored  

### 3️⃣ Curated Zone – Machine Learning Dataset
- Customer data is joined with accelerometer readings → keeps only users with valid sensor data **and consent**  
- Step Trainer IoT data is cleaned and moved into its **own trusted dataset**  
- Finally, **accelerometer + Step Trainer data** are combined (on `timestamp` and `serial number`) to create the **`machine_learning_curated` dataset**  

This curated dataset is used by **data scientists** to train **step detection models**.  

---
