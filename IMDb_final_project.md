# Procesamiento de Datos de Películas de IMDb

Este documento describe el proceso de ETL (Extracción, Transformación y Carga) aplicado a un conjunto de datos de películas de la mayor base de datos de peliculas a nivel mundial (IMDb), utilizando Apache Spark para la manipulación de datos y la generación de informes. El proceso se divide en tres capas principales:

## 1. Capa Inicial

- **Fuente de Datos:** El proceso comienza con la lectura de un archivo CSV ("IMDbMovies.csv") que contiene información sobre películas. Este archivo se encuentra en el directorio `/content/ProcesamientoDatos/01_capa_inicial/`.
- **Formato:** CSV
- **Procesamiento:** Se define un esquema para asegurar la correcta interpretación de los tipos de datos en el DataFrame. Se realiza una lectura del archivo csv utilizando el esquema definido. Los datos se cargan en un DataFrame de Spark.

```python
!pip install pyspark
!pip install matplotlib
!git clone "https://github.com/iansilva2305/ProcesamientoDatos.git"
```

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count, col, mean, min, max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import regexp_extract, col, regexp_replace, avg, sum
import matplotlib.pyplot as plt

# Initialize SparkSession
spark = (SparkSession.builder
                    .appName("Procesamiento_de_Datos")
                    .getOrCreate()
        )
```

```python
# Definiendo el esquema inicial con los tipos de datos adecuados (String) y el manejo de los datos entre comillas
schema = StructType([
    StructField("Title", StringType(), True),
    StructField("Sumary", StringType(), True),
    StructField("Director", StringType(), True),
    StructField("Writer", StringType(), True),
    StructField("Main_Genres", StringType(), True),
    StructField("Motion_Picture_Rating", StringType(), True),
    StructField("Runtime_Minutes", IntegerType(), True),
    StructField("Release_Year", StringType(), True),
    StructField("Rating", StringType(), True),
    StructField("Number_of_Ratings", StringType(), True),
    StructField("Budget", StringType(), True),
    StructField("Gross_US_Canada", StringType(), True),
    StructField("Gross_Worldwide", StringType(), True),
    StructField("Opening_Weekend_US_Canada", StringType(), True)
])

# Cargando los datos de IMDbMovies.csv en la carpeta "/coontent/ProcesamientoDatos/01_capa_inicial", la misma que debemos crear,
#utilizando el esquema especificado "schema" y el manejo de cadenas entre comillas
df = spark.read.csv("/content/ProcesamientoDatos/01_capa_inicial/IMDbMovies.csv", header=True, schema=schema, quote='"', escape='"')

# Mostrando las primeras filas del DataFrame
df.show(10)
```

## 2. Capa Intermedia

En esta capa se realiza la limpieza y transformación de los datos provenientes de la capa inicial. Se generan dos DataFrames:

### 2.1 DataFrame de Finanzas

- **Procesamiento:** Esta capa se centra en la limpieza y transformación de los datos relacionados con las finanzas de las películas.
  - Se extrae la información monetaria (moneda) utilizando expresiones regulares.
  - Se limpian las columnas numéricas ("Budget", "Gross_US_Canada", "Gross_Worldwide", "Opening_Weekend_US_Canada") utilizando expresiones regulares, reemplazando caracteres especiales y convirtiendo los valores a tipo numérico (double), manejo de valores nulos.
- **Destino:** Los datos transformados se guardan en formato Parquet en el directorio `/content/ProcesamientoDatos/02_capa_intermedia/finanzas.parquet`.

```python
# Definiendo los esquemas para migrar la data
months_abbr = r"(Ene|Feb|Mar|Abr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dic)"

finanzas_cleaned_df = (df
    .withColumn("Currency",regexp_extract(col("Budget"), "(?:[\\$€£¥]|\\b(?:USD|EUR|GBP|MXN)\\b)", 0))
    .withColumn("Budget", regexp_replace(col("Budget"), r"[^0-9.]", ""))
    .withColumn("Budget", when(col("budget") == "", None).otherwise(col("budget").cast("double")))
    .withColumn("Gross_US_Canada", regexp_replace(col("Gross_US_Canada"), "[$,]", ""))
    .withColumn("Gross_US_Canada", when(col("Gross_US_Canada") == "", None).otherwise(col("Gross_US_Canada").cast("double")))
    .withColumn("Gross_Worldwide", regexp_replace(col("Gross_Worldwide"), "[$,]", ""))
    .withColumn("Gross_Worldwide", when(col("Gross_Worldwide") == "", None).otherwise(col("Gross_Worldwide").cast("double")))
    .withColumn("Opening_Weekend_US_Canada", regexp_extract(col("Opening_Weekend_US_Canada"), r"^(.*?)(" + months_abbr + r")", 1))
    .withColumn("Opening_Weekend_US_Canada", regexp_replace(col("Opening_Weekend_US_Canada"), "[$,]", ""))
    .withColumn("Opening_Weekend_US_Canada", when(col("Opening_Weekend_US_Canada") == "", None).otherwise(col("Opening_Weekend_US_Canada").cast("double")))
    .select("Title", "Release_Year", "Currency", "Budget", "Gross_US_Canada", "Gross_Worldwide", "Opening_Weekend_US_Canada"))

#Imprimimos el esquema
finanzas_cleaned_df.printSchema()

#Imprimimos el DataFrame
print(finanzas_cleaned_df)

#Mostramos registros
finanzas_cleaned_df.show(10)

# Guardamos en formato Parquet la tabla de Finanzas
finanzas_cleaned_df.write.format("parquet").mode("overwrite").save("/content/ProcesamientoDatos/02_capa_intermedia/finanzas.parquet")
```

### 2.2 DataFrame de Calificaciones

- **Procesamiento:** Esta capa limpia y transforma datos relacionados con la calificación de las películas.
  - Se extrae la información de calificaciones, limpiando caracteres innecesarios y convirtiendo los valores a tipo numérico.
  - Los valores de la columna "Number_of_Ratings" se convierten a número enteros, manejando las abreviaturas ("K") para representar miles.
- **Destino:** Los datos transformados se guardan en formato Parquet en el directorio `/content/ProcesamientoDatos/02_capa_intermedia/calificaciones.parquet`.

```python
# Creando el Dataframe de Calificaciones
calificaciones_cleaned_df = df.select(col("Title"), col("Release_Year").cast("int").alias("Release_Year"),
                                      regexp_replace(col("Rating"), "/10", "").cast("double").alias("Average_Rating"),
                                      when(col("Number_of_Ratings").contains("K"), regexp_replace(col("Number_of_Ratings"), "K", "").cast("integer") * 1000)
                                      .otherwise(regexp_replace(col("Number_of_Ratings"), "[^0-9]", "").cast("integer")).alias("Number_of_Ratings")
                                      )

# Imprimimos el schema
calificaciones_cleaned_df.printSchema()

# Muestra los primeros 10 resultados
calificaciones_cleaned_df.show(10)

# Guarda en formato parquet la tabla de Calificaciones
calificaciones_cleaned_df.write.format("parquet").mode("overwrite").save("/content/ProcesamientoDatos/02_capa_intermedia/calificaciones.parquet")
```

## 3. Capa Final

En esta capa se realizan agregaciones y se guardan los datos finales.

### 3.1 Agregaciones

Se realizan agrupaciones utilizando las columnas "Title" y "Release_Year" como claves primarias compuestas en los DataFrames:

- **DataFrame de Finanzas:**
  - Se calcula la suma de "Budget", "Gross_US_Canada", "Gross_Worldwide", y "Opening_Weekend_US_Canada" para cada película.

```python
# Agrupación por clave primaria y agregación
finanzas_grouped_df = (finanzas_cleaned_df
              .groupBy("Title", "Release_Year")
              .agg(
                  sum("Budget").alias("Total_Budget"),
                  sum("Gross_US_Canada").alias("Total_US_Canada_Gross"),
                  sum("Gross_Worldwide").alias("Total_Worldwide_Gross"),
                  sum("Opening_Weekend_US_Canada").alias("Total_Opening_Weekend_Gross")
              )
             )

# Mostrar los resultados
print(f"El número de registros de finanzas agrupados es: {finanzas_grouped_df.count()}")

finanzas_grouped_df.printSchema()
finanzas_grouped_df.show()
```

- **DataFrame de Calificaciones:**
  - Se calcula el promedio de "Average_Rating" y la suma de "Number_of_Ratings" para cada película.

```python
calificaciones_cleaned_df.printSchema()
calificaciones_cleaned_df.show()
# Agrupación por clave primaria y agregación
calificaciones_grouped_df = (calificaciones_cleaned_df
              .groupBy("Title", "Release_Year")
              .agg(
                  avg("Average_Rating").alias("Average_Rating"),
                  sum("Number_of_Ratings").alias("Number_of_Ratings")
              )
             )

# Mostrar los resultados
print(f"El número de registros de calificaciones agrupados es: {calificaciones_grouped_df.count()}")

calificaciones_grouped_df.printSchema()
calificaciones_grouped_df.show()
```

### 3.2 Almacenamiento

- **DataFrame de Finanzas Agrupado:** Los datos agregados se almacenan en formato Parquet en `/content/ProcesamientoDatos/03_capa_final/finanzas_grouped.parquet`.

- **DataFrame de Calificaciones Agrupado:** Los datos agregados se almacenan en formato Parquet en `/content/ProcesamientoDatos/03_capa_final/calificaciones_grouped.parquet`.

```python
# Guarda en formato parquet la tabla de Finanzas Agrupada por Clave Primaria Compuesta (Title y Release_Year)
finanzas_grouped_df.write.format("parquet").mode("overwrite").save("/content/ProcesamientoDatos/03_capa_final/finanzas_grouped.parquet")

# Guarda en formato parquet la tabla de Calificaciones Agrupada por Clave Primaria Compuesta (Title y Release_Year)
calificaciones_grouped_df.write.format("parquet").mode("overwrite").save("/content/ProcesamientoDatos/03_capa_final/calificaciones_grouped.parquet")
```

## Análisis Exploratorio de Datos (EDA)

Se realiza un análisis exploratorio de datos (EDA) para ambos conjuntos de datos en la capa intermedia. Este análisis incluye:

- Estadísticas descriptivas (count, mean, stddev, min, max)
- Conteo de valores faltantes y ceros
- Conteo de valores distintos
- Visualizaciones (histogramas)

```python
# Carga los archivos parquet
finanzas_cleaned_df = spark.read.parquet("/content/ProcesamientoDatos/02_capa_intermedia/finanzas.parquet")
calificaciones_cleaned_df = spark.read.parquet("/content/ProcesamientoDatos/02_capa_intermedia/calificaciones.parquet")

#Definimos nuestra función para realizar el análisis EDA (Análsis Exploratorio de Datos)
def analisis_eda(df, df_name):
  """
Realiza el análisis EDA de un dataFrame especifico.
  """
  print(f"\nEDA Analisis para {df_name}:")
  for column in df.columns:
      if column not in ["Title", "Release_Year"]: # Excluimos las columnas que son la clave para este análisis
          print(f"\nColumna: {column}")

          # Manejar de posibles errores de tipo para la agregados numéricos
          try:
              df.select(column).describe().show()
              print("Valores faltantes:", df.filter(col(column).isNull()).count())
              print("Cuenta de ceros:", df.filter(col(column) == 0).count())
              df.select(count(when(isnan(col(column)), column)).alias(f"cuenta_nulos_{column}")).show() # Cuenta de valores NaN

          except Exception as e:
              print(f"Error en agregados númericos {column}: {e}")

          # Cuenta valores distintos
          print("Cuenta valores distintos:", df.select(column).distinct().count())

          # Crea visualizaciones
          # En caso aplique para columnas Categoricas mostramos un gráfico de Barras
          # En caso aplique mostramos un Histograma para valores númericos
          try:
              df.select(column).toPandas().plot(kind='hist', title=f'Distribución de la columna: {column}')
              plt.show()

          except Exception as e:
              print(f"No es posible crear histograma para la columna {column}: {e}")


# Realiza Análisis EDA en el DataFrame de Finanzas
analisis_eda(finanzas_cleaned_df, "Finanzas")

# Realiza Análisis EDA en el DataFrame de Calificaciones
analisis_eda(calificaciones_cleaned_df, "Calificaciones")
```

```python
#Detenemos el motor de procesamiento
spark.stop()
```

## Tecnologías Utilizadas

- Google Colab
- Apache Spark (PySpark)
- Matplotlib
