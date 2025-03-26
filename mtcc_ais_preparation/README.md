# **Módulo de Interpolación (`_interpolation.py`)**

## **Función: `pos_interp`**

### **Funcionalidad**
La función `pos_interp` realiza **interpolación lineal** de posiciones faltantes (latitud y longitud) en un conjunto de datos, con base en un **intervalo de tiempo uniforme**.
Rellena los valores faltantes entre registros consecutivos considerando una **restricción de tiempo** (máximo de **2 horas**) para determinar si se debe interpolar o conservar los valores originales.
El resultado es un **DataFrame de Spark** que contiene las posiciones interpoladas, junto con las **columnas especificadas** o todas las columnas si no se define un subconjunto.

---

## **Parámetros**
1. **`dfspark (Spark DataFrame)`**
   - DataFrame de entrada que contiene los datos.

2. **`interval_minutes (int)`**
   - Frecuencia (en minutos) para la interpolación.
   - Define el intervalo de tiempo entre puntos interpolados.

3. **`group (Opcional[listado de strings], default=None)`**
   - Columnas por las que se agruparán los datos para realizar la interpolación.
   - Si no se especifica, la función agrupa **automáticamente por `imo`**.

4. **`columns (Opcional[listado de strings], default=["*"])`**
   - Columnas que se desean conservar en el DataFrame de salida.
   - Si no se indica, se devuelven **todas las columnas** del DataFrame original.

---

## **Valor de Retorno**
- **`df_interpolated (Spark DataFrame)`**
  DataFrame limpio con los datos interpolados, que incluye:
  - **Latitud y longitud interpoladas** en intervalos definidos por `interval_minutes`.
  - Interpolación limitada a un **máximo de 2 horas** entre puntos consecutivos.
  - Conjunto de columnas limitado a las **especificadas** o incluye todas si no se definió ninguna.

---

## **Flujo Interno de Procesamiento**

### **1. Configuración Inicial**
- Calcula la **frecuencia de remuestreo** (`resample_interval`) en segundos a partir de `interval_minutes`.
- Si no se especifican columnas, se consideran todas las del DataFrame original.
- Se define una **cláusula de partición** para agrupar, ya sea por `imo, segr, flag` (si están presentes) o por las columnas indicadas en `group`.

### **2. Cálculo de Límites Temporales**
- Calcula los **timestamps de inicio y fin** (`PreviousTimestampRoundUp` y `NextTimestampRoundDown`) para cada segmento en función del intervalo definido.
- Estos límites se usan para generar **secuencias de tiempo uniformes** por grupo.

### **3. Interpolación Lineal**
- Genera puntos interpolados **equispaciados en el tiempo** dentro de los límites calculados.
- Para cada punto interpolado:
  - **Latitud (`latitude`)**:
    - Se interpola linealmente entre los valores previos (`latitude_lag`) y actuales (`latitude_old`) si la diferencia de tiempo **es menor a 2 horas**.
    - Si la diferencia **supera las 2 horas**, se conserva la **latitud original (`latitude_old`)**.
  - **Longitud (`longitude`)**:
    - Sigue el mismo procedimiento que la latitud.

### **4. Remuestreo por Frecuencia**
- Alinea los datos interpolados a intervalos uniformes (`interval_minutes`).
- Calcula **brechas de tiempo (`freq`)** entre puntos consecutivos:
  - Si la brecha supera las **2 horas**, se asigna `freq = 0`.

### **5. Limpieza y Salida de Datos**
- Elimina **columnas temporales** usadas durante el procesamiento.
- Devuelve el **DataFrame interpolado final**, listo para análisis o procesamiento posterior.

---

# **Módulo Espacial (`_spatial.py`)**

## **Función: `wkt_load`**

### **Funcionalidad**
La función `wkt_load` transforma datos espaciales en formato **Point (latitud, longitud)** a formato **Well-Known Text (WKT)**, y genera un **DataFrame de Spark** con objetos geométricos.
Este formato es necesario para realizar **filtros espaciales** con **Apache Sedona**.
No obstante, en muchas aplicaciones espaciales, los **hexágonos H3** pueden ser más eficientes.

---

## **Parámetros**
1. **`spark (SparkSession)`**
   - Sesión activa de Spark, necesaria para ejecutar consultas y gestionar el DataFrame.

2. **`df (DataFrame)`**
   - DataFrame de Spark que contiene datos tabulares con un campo **WKT** a transformar.

3. **`wkt_field (str, opcional, default='WKT_geom')`**
   - Nombre de la columna que contiene cadenas WKT.
   - Esta columna se usa para crear la columna de geometría.

4. **`drop_wkt (bool, opcional, default=True)`**
   - Define si se elimina la columna original WKT tras crear la geometría:
     - **`True`**: Elimina el campo WKT.
     - **`False`**: Conserva el campo WKT original en el DataFrame resultante.

---

## **Valor de Retorno**
- **`geom_df (DataFrame)`**
  Un **DataFrame de Spark** con una nueva columna `geom`, que contiene los objetos geométricos creados a partir del campo WKT.
  Si **`drop_wkt=True`**, el campo original WKT se elimina.

---

## **Flujo Interno de Procesamiento**

### **1. Crear Vista Temporal**
- Registra el DataFrame de entrada como una **vista SQL temporal** (`x`) para permitir consultas SQL sobre él.

### **2. Conversión de Campo WKT a Geometría**
- Utiliza la función **`ST_GeomFromWKT`** de **Apache Sedona** para transformar el campo WKT (`wkt_field`) en un **objeto de geometría**.
- El resultado se almacena en una nueva columna `geom`.

### **3. Validación del Tipo de Geometría**
- Imprime el **esquema del DataFrame resultante** y muestra el **primer registro** para verificar que se generaron correctamente los objetos geométricos.
- Ayuda a detectar errores en los datos de entrada, como **geometrías mal formadas**.

### **4. Devolver el DataFrame Procesado**
- Devuelve el **DataFrame de Spark procesado**, incluyendo la nueva columna `geom`.

---

# **Módulo `_vessel_specs_ghg4`**

## **Funciones**

### **1. `find_folder_csv(folder_name, file_name)`**

**Funcionalidad:**
Busca un archivo `.csv` dentro de una carpeta especificada (dentro del paquete) y lo carga como un DataFrame de Pandas.

#### **Parámetros:**
- `folder_name (str)`: Nombre de la carpeta donde se encuentra el archivo.
- `file_name (str)`: Nombre del archivo `.csv` a cargar (incluyendo la extensión).

#### **Valor de Retorno:**
- `df (pd.DataFrame)`: DataFrame con la información almacenada en el archivo CSV.

---

### **2. `clean_string(text)`**

**Funcionalidad:**
Elimina signos de puntuación de un texto y lo convierte a minúsculas.

#### **Parámetros:**
- `text (str)`: Texto de entrada.

#### **Valor de Retorno:**
- `str`: Texto procesado, sin puntuación y en minúsculas.

---

### **3. `compare_similarity(text)`**

**Funcionalidad:**
Compara la similitud entre el texto de entrada y una lista de tipos de buques estándar. Si la similitud es mayor al **75%**, devuelve el tipo correspondiente.

#### **Parámetros:**
- `text (str)`: Texto a comparar.

#### **Valor de Retorno:**
- `str`: Tipo de buque coincidente o `None` si no se encuentra coincidencia.

---

### **4. `bin_finder(vessel_t, value, df_in)`**

**Funcionalidad:**
Clasifica un tipo de buque dentro de su **IMO bin** correspondiente, según los rangos definidos en el informe **IMO GHG4**.

#### **Parámetros:**
- `vessel_t (str)`: Tipo de buque.
- `value (float)`: Valor de referencia para la clasificación (por ejemplo, DWT, GT, TEU).
- `df_in (pd.DataFrame)`: DataFrame con los rangos de clasificación.

#### **Valor de Retorno:**
- `int`: Número de bin IMO correspondiente, o `0` si no hay coincidencia.

---

## **Asignaciones Específicas**

### **Unidades de Carga por Tipo de Buque**
Define la unidad de carga estándar según el tipo de buque:
- **Deadweight (DWT)** para graneleros, tanqueros y buques de carga general.
- **TEU** para buques portacontenedores.
- **Gross Tonnage (GT)** para cruceros, ferries y otros buques especializados.

### **Tipos de Motor según Fuente de Energía**
Clasifica los buques según su **tipo de motor y fuente de energía**:
- **Motores a petróleo → Diesel, HFO**
- **Velas → Energía eólica auxiliar**
- **Turbinas de gas → Propulsión de alta velocidad**
- **Turbinas de vapor → Propulsión en buques especializados**

---

### **5. `check_json(url)`**

**Funcionalidad:**
Descarga y decodifica contenido JSON desde una URL especificada.

#### **Parámetros:**
- `url (str)`: URL desde la cual se obtiene el archivo JSON.

#### **Valor de Retorno:**
- `json_str (str)`: Cadena con el contenido JSON descargado y decodificado.

---

## **Función: `adapted_specs_imo`**

### **Funcionalidad:**
La función `adapted_specs_imo` ajusta las especificaciones de buques según los métodos del informe **IMO GHG4**.
Si un tipo de buque no está presente en el **Lloyd’s Fleet Register**, la función aplica **similitud coseno** para encontrar la mejor coincidencia desde una lista predefinida.
El resultado es un **DataFrame de Pandas** con especificaciones de buques estructuradas conforme a los estándares del IMO GHG4.

---

## **Parámetros**
1. `df_unique_imo (pd.DataFrame)`
   - DataFrame de entrada con registros AIS únicos combinados con especificaciones de buques provenientes de **IHS Markit**.

---

## **Valor de Retorno**
- `ind (pd.DataFrame)`
  Un DataFrame con las especificaciones de los buques AIS, ajustadas a los métodos del IMO GHG4, incluyendo las siguientes columnas adicionales:
  - `imobin`: Clasificación IMO bin según los bins reconocidos en el GHG4.
  - `fuel`: Tipo de combustible asignado al buque.
  - `meType`: Tipo de motor principal asignado.

---

## **Flujo Interno de Procesamiento**

### **1. Preparación Inicial**
- Renombra columnas para mantener consistencia (`vessel_type_main` → `ais_type`, etc.).
- Rellena valores faltantes en **`ShiptypeLevel5`** utilizando información de registros AIS.

### **2. Asignación del Tipo de Buque**
- Si **ShiptypeLevel5** no coincide con la lista estándar de tipos de buques, se aplica **similitud coseno** para encontrar la coincidencia más cercana.
- Se eliminan los buques que no logran una coincidencia válida.

### **3. Clasificación IMO Bin**
- Se fusionan los datos con la tabla estándar de tipos de buques.
- Se calcula el bin IMO con base en la capacidad de carga del buque (**DWT, GT, TEU**).

### **4. Asignación del Tipo de Combustible**
- Determina el **tipo de combustible** según la **propulsión** y categoría del GHG4.
- Si el tipo es desconocido, asigna un valor por defecto según el motor (`HFO`, `MDO`, `LNG`, etc.).

### **5. Asignación del Tipo de Motor**
- Clasifica el **tipo de motor** según su sistema de propulsión y revoluciones por minuto (RPM).
- Asigna categorías como:
  - **SSD (Slow Speed Diesel)**
  - **MSD (Medium Speed Diesel)**
  - **HSD (High Speed Diesel)**
  - **Turbina de Vapor**
  - **Turbina de Gas**
  - **Velas**
  - **No Propulsado**, etc.

### **6. Validaciones Finales y Retorno**
- Elimina **columnas intermedias** usadas en el procesamiento.
- Devuelve el **DataFrame final** procesado y listo para uso.

---
