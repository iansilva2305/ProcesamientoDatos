# Procesamiento de Datos de Películas

Este documento describe el proceso de ETL (Extracción, Transformación y Carga) aplicado a un conjunto de datos de películas, utilizando Apache Spark para la manipulación de datos y la generación de informes. El proceso se divide en tres capas principales:

## 1. Capa Inicial

- **Fuente de Datos:** El proceso comienza con la lectura de un archivo CSV ("IMDbMovies.csv") que contiene información sobre películas. Este archivo se encuentra en el directorio `/content/01_capa_inicial/`.
- **Formato:** CSV
- **Procesamiento:** Se define un esquema para asegurar la correcta interpretación de los tipos de datos en el DataFrame. Se realiza una lectura del archivo csv utilizando el esquema definido. Los datos se cargan en un DataFrame de Spark.

## 2. Capa Intermedia

En esta capa se realiza la limpieza y transformación de los datos provenientes de la capa inicial. Se generan dos DataFrames:

### 2.1 DataFrame de Finanzas

- **Procesamiento:** Esta capa se centra en la limpieza y transformación de los datos relacionados con las finanzas de las películas.
  - Se extrae la información monetaria (moneda) utilizando expresiones regulares.
  - Se limpian las columnas numéricas ("Budget", "Gross_US_Canada", "Gross_Worldwide", "Opening_Weekend_US_Canada") utilizando expresiones regulares, reemplazando caracteres especiales y convirtiendo los valores a tipo numérico (double), manejo de valores nulos.
- **Destino:** Los datos transformados se guardan en formato Parquet en el directorio `/content/02_capa_intermedia/finanzas.parquet`.

### 2.2 DataFrame de Calificaciones

- **Procesamiento:** Esta capa limpia y transforma datos relacionados con la calificación de las películas.
  - Se extrae la información de calificaciones, limpiando caracteres innecesarios y convirtiendo los valores a tipo numérico.
  - Los valores de la columna "Number_of_Ratings" se convierten a número enteros, manejando las abreviaturas ("K") para representar miles.
- **Destino:** Los datos transformados se guardan en formato Parquet en el directorio `/content/02_capa_intermedia/calificaciones.parquet`.

## 3. Capa Final

En esta capa se realizan agregaciones y se guardan los datos finales.

### 3.1 Agregaciones

Se realizan agrupaciones utilizando las columnas "Title" y "Release_Year" como claves primarias compuestas en los DataFrames:

- **DataFrame de Finanzas:**
  - Se calcula la suma de "Budget", "Gross_US_Canada", "Gross_Worldwide", y "Opening_Weekend_US_Canada" para cada película.

- **DataFrame de Calificaciones:**
  - Se calcula el promedio de "Average_Rating" y la suma de "Number_of_Ratings" para cada película.

### 3.2 Almacenamiento

- **DataFrame de Finanzas Agrupado:** Los datos agregados se almacenan en formato Parquet en `/content/03_capa_final/finanzas_grouped.parquet`.

- **DataFrame de Calificaciones Agrupado:** Los datos agregados se almacenan en formato Parquet en `/content/03_capa_final/calificaciones_grouped.parquet`.

## Análisis Exploratorio de Datos (EDA)

Se realiza un análisis exploratorio de datos (EDA) para ambos conjuntos de datos en la capa intermedia. Este análisis incluye:

- Estadísticas descriptivas (count, mean, stddev, min, max)
- Conteo de valores faltantes y ceros
- Conteo de valores distintos
- Visualizaciones (histogramas)

## Tecnologías Utilizadas

- Google Colab
- Apache Spark (PySpark)
- Matplotlib
