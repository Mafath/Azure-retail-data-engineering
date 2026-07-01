<h1 align="center">
Azure Databricks End-to-End Retail Data Engineering Project</h1>

An enterprise-grade, end-to-end data engineering solution built on Azure and Databricks. Project demonstrates how to transition from traditional hobby/batch architectures to production-ready, real-time incremental pipelines implementing data governance, modern optimization techniques, and a complete Medallion Architecture.

## 📌 Project Overview
Project mirrors production ecosystems by establishing native data connectivity, cloud asset orchestration, security parameters, and cross-application governance.

The architecture processes transactional retail data streams (Orders, Customers, Products, Regions mapping) through **Bronze**, **Silver**, and **Gold** zones, culminating in an optimized Lakehouse serving analytics consumers like Power BI.


##  Architecture & Technology Stack

* **Cloud Infrastructure:** Microsoft Azure (Storage Account/ADLS Gen2, Azure Identity & Access Management)
* **Data Governance & Security:** Databricks Unity Catalog (MetaStore management, Storage Credentials, External Locations)
* **Data Ingestion:** Databricks Autoloader (built on Apache Spark Structured Streaming) & Code-Free Automated Data Ingest
* **Data Processing & Orchestration:** PySpark, Python OOP, Databricks Workflows (Dynamic multi-task loops, parallel processing)
* **Storage Framework:** Delta Lake Format & Optimized Apache Parquet
* **Advanced Data Modeling:** Delta Live Tables (DLT), Slowly Changing Dimensions (SCD Type 1 & Type 2), Star Schema (Fact & Dimension Tables)

---

<h1 align="center">
Project Walkthrough
</h1>

## 1. Cloud Infrastructure Setup & Hierarchy
Below is the provisioned resource group environment containing the dedicated Azure Databricks Service, the unified Storage Account, and the Access Connector managing identity wrappers securely:


<table align="center">
  <tr>
    <td align="center">
      <img src="Images/Resource%20Group%20Overview.png" alt="Description">
      <br>
      <b>Azure Resource Group</b>
    </td>
  </tr>
</table>


## 2. Medallion Pipeline & Implementation Details

### i. Unified Governance Setup (Unity Catalog)
* **MetaStore Architecture:** Established a centralized Unity Catalog MetaStore mapped to a dedicated root ADLS Gen2 container (`meta-store`) to enforce structural isolation.
* **External Locations:** Managed distinct IAM permission wrappers via Azure Access Connectors. Configured secure storage credentials to mount lakehouse zones natively without hardcoding secret keys.


| Azure Datalake | Databricks Workspace |
| :---: | :---: |
| ![Bronze Incremental Pipeline-i](Images/Datalake%20Storage.png) | ![Bronze Incremental Pipeline-ii](Images/Databricks%20Workspace%20Overview.png) |


### ii. Source Container
* **Source Landing Zone:** RAW landing directory hosting landing file structures before ingestion:

<table align="center">
  <tr>
    <td align="center">
      <img src="Images/Source%20Container.png" alt="Description">
      <br>
      <b>Data Sources</b>
    </td>
  </tr>
</table>


### iii. Ingestion Layer (Bronze)


* **Code-Free Baseline Ingestion:** Leveraged Databricks' automated interface to pull static mapping metadata (`regions`) directly into cataloged tables in two clicks.
* **Dynamic Autoloader Pipeline:** Engineered a robust Spark Structured Streaming ingestion engine using `cloudfiles` to track and automatically infer incoming schemas. Orchestrated through Databricks Workflows with a specialized loop task (`For Each`) feeding an itemized dataset array dynamically.

| Bronze Run Duration Profiles | Bronze Workflow Looping Logic |
| :---: | :---: |
| ![Bronze Incremental Pipeline-i](Images/Bronze%20Incremental%20Pipeline-i.png) | ![Bronze Incremental Pipeline-ii](Images/Bronze%20Incremental%20Pipeline-ii.png) |

* **Idempotency & Fault Tolerance:** Configured specialized `checkpointLocation` options utilizing persistent RocksDB state-stores inside the bronze tier. This ensures strictly "Exactly-Once" stream semantics—even across abrupt cluster restarts, only freshly appended micro-batches are processed.

<table align="center">
  <tr>
    <td align="center">
      <img src="Images/Bronze%20Container.png" alt="Description">
      <br>
      <b>Bronze Container | Medallion Architecture</b>
    </td>
  </tr>
</table>


* Bronze layer related notebooks

📃 **<a href="Notebooks/Bronze_Layer.ipynbipynb">Bronze Layer</a>** <br>
📃 **<a href="Notebooks/Dynamic Notebook.ipynb">Dynamic Notebook</a>** <br>
📃 **<a href="Notebooks/Parameters.ipynb">File Parameters</a>**




### iv. Transformation & Enrichment Layer (Silver)
* **Data Laundering:** Schema validation, dropping toxic metadata parameters (e.g., handling autoloader `_rescued_data`), and executing structural optimizations.
* **PySpark Windowing & Reusability Classes:** Applied complex algorithmic patterns (`Dense Rank`, `Rank`, and `Row Number`) partitioned chronologically across transaction tracking frameworks. Abstracted these mechanics into reusable Python OOP structures (`Windows` classes) to build industrial-grade code modularity.

<table align="center">
  <tr>
    <td align="center">
      <img src="Images/Silver%20Container.png" alt="Description">
      <br>
      <b>Silver Container | Medallion Architecture</b>
    </td>
  </tr>
</table>


* **String & Array Processing:** Programmed advanced domain extraction models utilizing regex splits to map behavioral target fields (e.g., profiling top customer communications domains across Gmail, Hotmail, and Yahoo). Conjointly merged relational identity states via `concat` string operations.
* **Cataloged Functions:** Built and registered specialized Unity Catalog functions (`Upper Funk`) executing custom language extensions (Python/SQL) for cross-team business logic reusability.

* Silver layer related notebooks

📃 **<a href="Notebooks/Silver_Orders.ipynb">Silver_Orders</a>** <br>
📃 **<a href="Notebooks/Silver_Customers.ipynb">Silver_Customers</a>** <br>
📃 **<a href="Notebooks/Silver_Products.ipynb">Silver_Products</a>** <br>
📃 **<a href="Notebooks/Silver_Regions.ipynb">Silver_Regions</a>**


### v. Data Modeling & Presentation Layer (Gold)



* **SCD Type 1 Framework:** Designed low-latency dimension layers tracking real-time status updates through conditional UPSERT logic.

📃 **<a href="Notebooks/Gold_Customers.ipynb">Gold_Customers</a>**

* **Delta Live Tables (SCD Type 2):** Automated historical data versioning via DLT pipelines utilizing `apply_changes` logic. This seamlessly logs state progression across time boundaries while handling deep lineage automation.

📃 **<a href="Notebooks/Gold_Products.ipynb">Gold_Products</a>**


<table align="center">
  <tr>
    <td align="center">
      <img src="Images/SCD%20Type%20II%20Pipeline.png" alt="Description">
      <br>
      <b>SCD Type II Pipeline</b>
    </td>
  </tr>
</table>


* **Analytical Star Schema:** Modeled structural data streams into optimized Fact tables and isolated analytical dimensions within Databricks Catalog explorer.

📃 **<a href="Notebooks/Gold_Orders.ipynb">Gold_Orders</a>**


<table align="center">
  <tr>
    <td align="center">
      <img src="Images/Databricks%20Catalog.png" alt="Description">
      <br>
      <b>Databricks Catalog</b>
    </td>
  </tr>
</table>

* **Lakehouse Provisioning:** Shared optimized SQL Warehouse endpoints with Power BI, decoupling heavy computational operations from end-user reporting logic.


<table align="center">
  <tr>
    <td align="center">
      <img src="Images/Gold%20Container.png" alt="Description">
      <br>
      <b>Gold Container | Medallion Architecture</b>
    </td>
  </tr>
</table>



## 3. Infrastructure Configuration & Workflow Orchestration

### Cluster Strategy
* **Development Layer:** Single-node runtime instances utilizing `15.4 LTS` long-term support profiles with automated 20-minute resource reclamation policies.
* **Production Execution:** Serverless architectures applied assignment-wide for light batch processing, shifting back to multi-core Job-specific engines for resource-intensive streaming environments.

### Complete Orchestration Lineage (Master Pipeline)
The final workflow layout processes downstream operations in parallel, systematically decoupling the ingestion runtimes before feeding concurrent Silver transformations and ultimate Gold Star-Schema aggregations.


<table align="center">
  <tr>
    <td align="center">
      <img src="Images/Jobs%20and%20Pipelines%20Overview.png" alt="Description">
      <br>
      <b>Jobs & Pipelines Overview</b>
    </td>
  </tr>
</table>

<table align="center">
  <tr>
    <td align="center">
      <img src="Images/Master%20Pipeline.png" alt="Description">
      <br>
      <b>Master Pipeline</b>
    </td>
  </tr>
</table>


##  4. Key Milestones

1.  **Administrative Mastery:** Deep competence in production environments by building real cloud identity access structures rather than writing basic Python code inside standalone notebooks.
2.  **Schema Evolution:** Advanced autoloader strategies to seamlessly handle downstream alterations using specialized rescue schema directories.
3.  **Parallel Execution Frameworks:** Maximized data compute optimization by building parallel execution graphs inside Databricks Workflows, reducing pipeline processing time.
4.  **Production-Grade Architecture:** Designed comprehensive historical tracking pipelines utilizing unified Delta Lake architectures and modular Python OOP practices.
