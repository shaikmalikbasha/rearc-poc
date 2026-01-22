# Rearc Quest – Proof of Concept (Local / Open-Source Implementation)

## Overview

This repository contains my implementation of the **Rearc Quest take-home assessment**.  
Because I did _not_ have an AWS account and needed to avoid external API tracking from a corporate machine, the solution uses **open-source emulations of AWS services** (MinIO, Flask, FastAPI) and a **local container-first architecture**.

The system demonstrates:

- Event-driven ingestion of data
- Object storage and notifications
- Webhook-triggered processing
- Lambda-style sync from public endpoints into storage
- Analytics using a Jupyter notebook

---

## Architecture Summary

The implementation mimics a typical AWS event-based pipeline while using **local tooling**:

1. **Public API sync** — Lambda-like service periodically fetches data from endpoints and stores into an S3-compatible store.
2. **Object ingestion** — Files are placed into MinIO (S3-compatible storage).
3. **Event notification** — MinIO webhook triggers an HTTP endpoint on object creation.
4. **Webhook receiver** — Receives events and passes through for processing.
5. **Processing services** — Handle business logic and subsequent storage.
6. **Data analysis** — Jupyter notebook addresses analytical requirements.

---

## AWS Service Mapping

| AWS Service                         | Equivalent in this Project               |
| ----------------------------------- | ---------------------------------------- |
| **Amazon S3**                       | **MinIO** (S3-compatible object storage) |
| **S3 Event Notifications**          | **MinIO Webhook**                        |
| **AWS Lambda**                      | **Lambda services**                      |
| **Terraform**                       | **Docker Compose orchestration**         |
| **External API (BLS)**              | **Mock BLS API service**                 |
| **Analytics (Athena / EMR / Glue)** | **Jupyter Notebook in `analysis-app/`**  |

---

## Repository Structure

```text
rearc-poc/
├── analysis-app/      # Jupyter notebook & academic analysis (Part 3)
├── bls-app/           # Mock BLS API server (Flask)
├── lambda-api/        # Lambda-style sync service from public endpoints
├── minio/             # MinIO configuration & setup for S3 emulation
├── s3-webhook/        # Webhook receiver for storage notifications
├── docker-compose.yml # Docker Compose stack for all services
├── QUEST.md           # Answers & assessment responses
└── .gitignore
```

## QUEST Requirement Mapping

This section maps each requirement from the **Rearc Quest** to the corresponding implementation in this repository.

---

### **Part 1 – Data Ingestion & Storage**

**QUEST Requirement**

- Ingest data from public endpoints
- Store raw data in Amazon S3
- Automate ingestion using serverless components

**Implementation**

- `lambda-api/` acts as a **Lambda-style service**
  - Periodically fetches data from public endpoints
  - Writes raw payloads into MinIO buckets
- `minio/` provides **S3-compatible object storage**

**AWS Mapping**

- AWS Lambda → `lambda-api/`
- Amazon S3 → MinIO

---

### **Part 2 – Event-Driven Processing**

**QUEST Requirement**

- Trigger downstream processing on object creation
- Use S3 event notifications
- Decouple ingestion and processing

**Implementation**

- MinIO is configured to emit **HTTP webhook notifications** on object creation
- `s3-webhook/` receives and processes these events
- Processing logic is isolated from ingestion logic

**AWS Mapping**

- S3 Event Notifications → MinIO Webhook
- EventBridge / Lambda → Webhook receiver + FastAPI Websockets

---

### **Part 2 – External API Integration (BLS)**

**QUEST Requirement**

- Enrich or process data using the BLS public API

**Implementation**

- `bls-app/` provides a **Flask-based mock of the BLS API**
- Preserves request/response behavior of the real API
- Used instead of the real BLS API to avoid:
  - IP logging
  - Request tracking from a corporate-managed device

**AWS Mapping**

- External BLS HTTP API → Mock Flask service

---

### **Part 3 – Analysis & Insights**

**QUEST Requirement**

- Perform analysis on ingested data
- Demonstrate data exploration and insight generation

**Implementation**

- `analysis-app/` contains a **Jupyter notebook**
- Addresses all analytical questions from **Part 3**
- Notebook-based workflow intentionally separated from pipeline logic with outputs

**AWS Mapping**

- Athena / Glue / EMR → Jupyter Notebook

---

### **Infrastructure & Deployment**

**QUEST Expectation**

- Infrastructure as Code
- Reproducible environment

**Implementation**

- Entire stack is defined in `docker-compose.yml`
- One-command local startup
- Clear service boundaries and reproducibility

**AWS Mapping**

- Terraform → Docker Compose (local IaC)
- ECS / Lambda → Containerized Lambda API services

---

### **Summary**

Although implemented locally, this solution:

- Preserves all architectural intent of the QUEST
- Uses open-source, AWS-compatible components
- Demonstrates system design, event-driven thinking, and engineering judgment

The architecture can be migrated to AWS with minimal changes.
