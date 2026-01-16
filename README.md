# Databricks_Medallion_FlightData_Project
Databricks Medallion ETL project (Raw → Bronze → Silver → Gold). Upload raw CSVs, ingest to Bronze via Workflow job using incremental streaming (readStream/writeStream), transform in Silver with Lakeflow Declarative Pipeline (Python modules), then build Gold fact and dim tables with SCD logic.
