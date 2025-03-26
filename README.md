# emissions_mtcc_open

Estas herramientas procesan datos AIS para la estimación de emisiones y el análisis operativo de embarcaciones. Al transformar datos en crudo en agregados estadísticos, facilitan la generación de información estructurada y optimizada para su uso en la Plataforma Global de las Naciones Unidas (UNGP).


## Tabla de Contenidos
- [Instalación](#instalación)
- [Uso](#uso)
- [Características](#características)
- [Contribuciones](#contribuciones)
- [Licencia](#licencia)
---

## Instalación

Vía el archivo `setup.py`

---

## Uso / Ejemplos

---

## **Ejemplo 1: `_vessel_specs_ghg4.py`**

```python
from mtcc_ais_preparation._vessel_specs_ghg4 import adapted_specs_imo
# Crear un DataFrame AIS de ejemplo
df_ais = pd.DataFrame({
    "imo": [1234567],
    "mmsi": [987654321],
    "ShiptypeLevel5": ["General Cargo"],
    "FuelType1First": ["Residual Fuel"],
    "FuelType2Second": ["Distillate Fuel"],
    "MainEngineRPM": [500],
    "PropulsionType": ["Oil Engine(s), Direct Drive"]
})

df_ajustado = adapted_specs_imo(df_ais)
print(df_ajustado.head())
```

---

## **Ejemplo 2: `Limpieza de nombres de tipo de buque`**
```python
from mtcc_ais_preparation.vessel_specs_ghg4 import clean_string

texto = "Bulk-Carrier!"
texto_limpio = clean_string(texto)

print(texto_limpio)  # Salida esperada: "bulkcarrier"
```

---

## Estructura del Repositorio
```
├── emissions_mtcc/
│   ├── __init__.py
│   │
│   ├── kalman_interpolation/
│   │   ├── __init__.py
│   │   ├── _kalman.py
│   │   ├── .gitkeep
│   │   ├── functions.py
│   │   └── README.md
│   │
│   ├── mtcc_ais_preparation/
│       ├── __init__.py
│       ├── _interpolation.py
│       ├── _spatial.py
│       ├── _vessel_specs_ghg4.py
│       ├── .gitkeep
│       ├── functions.py
│       ├── README.md
│       │
│       ├── ghg4_tables/
│       │   ├── __init__.py
│       │   ├── table_17.csv
│       │   ├── table_18.csv
│       │   ├── table_19_1.csv
│       │   ├── table_19_2.csv
│       │   ├── table_21.csv
│       │   ├── table_44.csv
│       │   ├── table_55_56.csv
│       │   └── table_59_60.csv
│       │
│       ├── polygons/
│       │   ├── __init__.py
│       │   ├── .gitkeep
│       │   ├── h3_coast_1nm.csv
│       │   ├── h3_coast_5nm.csv
│       │   ├── h3_eca_reg13_nox.csv
│       │   ├── h3_eca_reg14_sox_pm.csv
│       │   ├── h3_wpi_port_polys_1NM.csv
│       │   ├── h3_wpi_port_polys_5NM.csv
│       │   ├── invalid_res_8.csv
│       │   ├── land_eez_wkt.csv
│       │   ├── panama_canal.csv
│       │   └── world_map.cvs
│       │
│       ├── vessel_types_adjust/
│       │   ├── __init__.py
│       │   ├── .gitkeep
│       │   ├── map_vessel_type_imo4.csv
│       │   ├── type_table.csv
│       │
│
├── CONTRIBUTING.md
├── MANIFEST.in
├── README.md
└── setup.py

```


## Características

### **`efficiency_functions/`**

Módulo enfocado en mejorar la eficiencia computacional en el procesamiento de datos. Incluye métodos para reducir errores en la estimación de trayectorias y emisiones.

### **`emissions_functions/`**

El núcleo del paquete, responsable del análisis y transformación de datos **AIS**. Contiene funciones para interpolación de trayectorias, ajustes de especificaciones de embarcaciones según el **IMO GHG4**, e integración de datos geoespaciales.

### **`ghg4_tables/`**

Repositorio de tablas de referencia utilizadas para la estimación de emisiones. Estas tablas incluyen información estructurada sobre diferentes tipos de embarcaciones y sus parámetros operacionales.

### **`polygons/`**

Conjunto de archivos geoespaciales que definen límites marítimos, incluyendo zonas portuarias,**identificación de Zonas de Control de Emisiones (ECA)** y análisis de rutas comerciales estratégicas.

---

## Contribuciones
- Gabriel Moisés Fuentes
- Martín Gómez Torres

---

## Licencia

Este proyecto está licenciado bajo [la Licencia MIT](./LICENSE).


