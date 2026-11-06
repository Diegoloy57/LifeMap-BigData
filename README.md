# Big Data Geoespacial – Dask, MongoDB, Spark, Flask y Jenkins

## 1. Descripción del proyecto

Este proyecto implementa una arquitectura de procesamiento Big Data orientada al análisis geoespacial de registros de criminalidad de Los Ángeles.

El objetivo principal es construir un flujo automatizado que permita:

- Descargar un dataset real desde Kaggle.
- Procesar más de un millón de registros.
- Limpiar y transformar coordenadas geográficas mediante Dask.
- Almacenar la información procesada en MongoDB utilizando GeoJSON.
- Crear índices geoespaciales `2dsphere`.
- Ejecutar procesamiento distribuido con Apache Spark.
- Generar agregaciones temporales y geográficas.
- Consultar la información mediante una API Flask.
- Automatizar construcción, pruebas y despliegue mediante Jenkins.
- Comparar el rendimiento de Dask y Spark utilizando diferentes configuraciones de workers.

La solución fue construida completamente sobre contenedores Docker para facilitar la reproducción del entorno.

---

## 2. Dataset

El dataset utilizado proviene de Kaggle:

**Crime Data from 2020 to Present**

Repositorio Kaggle:

`utkarsh1093/crime-data-from-2020-to-nov2025`

Archivo utilizado:

`Crime_Data_from_2020_to_Present.parquet`

Características utilizadas en el proyecto:

- Registros originales: **1,004,991**
- Registros válidos después de limpieza: **1,002,751**
- Registros descartados: **2,240**
- Formato: Parquet
- Tamaño aproximado: 42 MB
- Incluye coordenadas de latitud y longitud.

Aunque el archivo comprimido no supera 1 GB, el dataset cumple el criterio de volumen del proyecto porque contiene más de un millón de registros.

La descarga se realiza automáticamente mediante la API de Kaggle.

Las credenciales de Kaggle no se almacenan dentro del repositorio. En Jenkins se configuran mediante Credentials y se inyectan durante la ejecución del pipeline.

---

## 3. Arquitectura

La arquitectura implementada está compuesta por los siguientes componentes:

```text
                ┌─────────────────────┐
                │       Kaggle        │
                │      Dataset        │
                └─────────┬───────────┘
                          │
                          ▼
                ┌─────────────────────┐
                │        Dask         │
                │ Scheduler + Workers │
                └─────────┬───────────┘
                          │
                    Limpieza / ETL
                          │
                          ▼
                ┌─────────────────────┐
                │      MongoDB        │
                │ GeoJSON + 2dsphere  │
                └─────────┬───────────┘
                          │
                    Lectura distribuida
                          │
                          ▼
                ┌─────────────────────┐
                │       Spark         │
                │ Master + Worker     │
                └─────────┬───────────┘
                          │
                 Agregaciones analíticas
                          │
                          ▼
                ┌─────────────────────┐
                │      MongoDB        │
                │ Nuevas colecciones  │
                └─────────┬───────────┘
                          │
                          ▼
                ┌─────────────────────┐
                │      Flask API      │
                │ Consultas REST      │
                └─────────────────────┘

                          ▲
                          │
                ┌─────────────────────┐
                │      Jenkins        │
                │       CI/CD         │
                └─────────────────────┘s
