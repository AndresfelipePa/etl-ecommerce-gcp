# etl-ecommerce-gcp
Este proyecto consume datos desde dos endpoints de una API REST y los carga en tablas relacionales en BigQuery. Utiliza Cloud Composer como orquestador para ejecutar un pipeline ETL con autenticación, extracción, transformación y carga automatizada en GCP.


# Requisitos previos:
- Tener habilitados los servicios Cloud Composer, BigQuery y Cloud Storage en GCP.
- Haber creado las tablas destino en BigQuery.
- Tener configurado el entorno y bucket de Cloud Composer.

# Pasos:
1. Subir el DAG `.py` al bucket `/dags/` del entorno Composer.
2. Activar el DAG desde la interfaz de Airflow.
3. Verificar la ejecución y los logs en Airflow.
4. Validar los datos cargados en BigQuery.

Stack tecnológico usado:
-Google Cloud Platform (BigQuery, Cloud Composer, Cloud Storage)
-Apache Airflow
-Python 3
-API REST externa (AWS App Runner)

-- Tabla products
CREATE TABLE `prueba-data-engineer.data.products` (
  id INT64,
  name STRING,
  price FLOAT64,
  created_at DATE
);

-- Tabla purchases
CREATE TABLE `prueba-data-engineer.data.purchases` (
  id STRING,
  status STRING,
  creditCardNumber STRING,
  creditCardType STRING,
  purchaseDate DATE
);

-- Tabla purchase_products
CREATE TABLE `prueba-data-engineer.data.purchase_products` (
  purchase_id STRING,
  product_id INT64,
  discount FLOAT64,
  quantity INT64
);
