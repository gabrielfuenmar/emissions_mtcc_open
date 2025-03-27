# **Funciones de Eficiencia**

## **Módulo Kalmar (`kalmar.py`)**

### **1. `prediction(x, y, vx, vy, dt, ax, ay)`**

**Funcionalidad:**
Predice la posición y velocidad futura de un objeto en un sistema bidimensional utilizando un modelo cinemático.

#### **Parámetros:**
- `x (float)`: Posición actual en el eje X (longitud).
- `y (float)`: Posición actual en el eje Y (latitud).
- `vx (float)`: Velocidad actual en el eje X.
- `vy (float)`: Velocidad actual en el eje Y.
- `dt (float)`: Intervalo de tiempo para la predicción.
- `ax (float)`: Aceleración en el eje X.
- `ay (float)`: Aceleración en el eje Y.

#### **Valor de Retorno:**
- `X_prime (numpy.ndarray)`: Una matriz de **4×1** que contiene las posiciones y velocidades predichas en X e Y.

---

### **2. `covariance(sigma1, sigma2, sigma3, sigma4)`**

**Funcionalidad:**
Genera una matriz de covarianza basada en las desviaciones estándar (**sigma**) de diferentes variables. La varianza modela los errores esperados de medición en los equipos que transmiten la información.

#### **Parámetros:**
- `sigma1 (float)`: Desviación estándar de la primera variable.
- `sigma2 (float)`: Desviación estándar de la segunda variable.
- `sigma3 (float)`: Desviación estándar de la tercera variable.
- `sigma4 (float)`: Desviación estándar de la cuarta variable.

#### **Valor de Retorno:**
- `cov_matrix (numpy.ndarray)`: Una matriz de covarianza de **4×4** donde:
  - Los **valores diagonales** representan las varianzas (**sigma²**) de las variables.
  - Los **valores fuera de la diagonal** representan las covarianzas entre variables.

---

### **3. `extrapolate(sample, freq=10)`**

**Funcionalidad:**
La función realiza los siguientes pasos:
1. Convierte posiciones geográficas (latitud y longitud) a coordenadas cartesianas en el sistema UTM.
2. Estima posiciones y velocidades mediante un enfoque de predicción iterativa.
3. Evalúa los errores de estimación y aplica un **filtro de Kalman** para suavizar los datos.
4. Reconstruye los datos en términos geográficos y agrega información faltante, como **Velocidad sobre el suelo (SOG)** y **Rumbo sobre el suelo (COG)**.
5. Crea un nuevo conjunto de datos con muestreo uniforme y devuelve el resultado refinado.

#### **Parámetros:**
- `sample (DataFrame)`: Conjunto de datos de entrada que contiene variables posicionales, de velocidad y otras relevantes para un objeto en movimiento (por ejemplo, embarcaciones). Se esperan las siguientes columnas:
  - `longitude (float)`: Longitud en grados.
  - `latitude (float)`: Latitud en grados.
  - `sog (float)`: Velocidad sobre el suelo.
  - `vx, vy (float)`: Componentes de velocidad en los ejes X e Y, respectivamente.
  - `ax, ay (float)`: Componentes de aceleración en los ejes X e Y, respectivamente.
  - `dt_pos_utc (datetime)`: Marca de tiempo asociada a cada registro.
  - Otros metadatos, como **IMO, MMSI**, etc.
- `freq (int, opcional)`: Frecuencia de muestreo en minutos para la extrapolación de datos (por defecto **10 minutos**).

#### **Valor de Retorno:**
- `sample (DataFrame)`: Conjunto de datos limpio que contiene datos procesados y suavizados con las siguientes características:
  - Datos interpolados a una frecuencia constante especificada por `freq`.
  - Variables adicionales estimadas y suavizadas, como **longitud, latitud, sog y cog**.
  - Columnas originales restauradas y reorganizadas, preservando el formato inicial del conjunto de datos.

---

### **Flujo Interno de Procesamiento**

#### **1. Preparación Inicial:**
- La función verifica si las columnas `anchoring_group` o `port_group` tienen un valor igual a **1** para proceder con el procesamiento.
- Se copian las columnas originales para mantener la estructura inicial.

#### **2. Conversión a Coordenadas UTM:**
- Se utiliza la librería **pyproj (Proj)** para transformar **latitud y longitud** en coordenadas cartesianas **UTM**, lo que permite cálculos precisos de distancia y velocidad.

#### **3. Predicción Inicial:**
- Se realiza una predicción inicial de posiciones y velocidades basada en las **aceleraciones (`ax, ay`)** y **velocidades (`vx, vy`)** conocidas.
- Se estiman los errores de predicción en coordenadas y velocidades.

#### **4. Cálculo de Matriz de Covarianza:**
- Se evalúan los errores en posiciones (**X, Y**) y velocidades (**vx, vy**) y se utilizan para construir la **matriz de covarianza `P`**.
- Se define una segunda matriz de covarianza **`R`** para modelar errores de medición con valores constantes.

#### **5. Aplicación del Filtro de Kalman:**
- Se inicializa un **filtro de Kalman** con matrices de transición, observación y covarianza.
- Un proceso iterativo de suavizado ajusta las posiciones y velocidades observadas, minimizando los errores.

#### **6. Transformación Inversa:**
- Las coordenadas **UTM suavizadas** se transforman nuevamente a **latitud y longitud** para mantener el formato geográfico original.

#### **7. Interpolación y Estructuración Final:**
- Los datos se interpolan a una frecuencia uniforme utilizando **pandas**.
- Se restauran las columnas originales y se eliminan las variables intermedias calculadas.
- Los valores faltantes en algunas columnas se completan usando el método **`ffill` (forward fill)** para completar el DataFrame.

---
