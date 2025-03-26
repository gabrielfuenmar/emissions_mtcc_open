# Interpolation Module (`_interpolation.py`)

## **Function: `pos_interp`**

### **Functionality**
The `pos_interp` function performs **linear interpolation** of missing positions (latitude and longitude) in a dataset based on a **uniform time interval**.
It fills missing values between consecutive records while considering **time constraints** (a maximum difference of **2 hours**) to determine whether to interpolate or keep the original values.
The result is a **Spark DataFrame** containing interpolated positions, along with either **specified columns** or all columns if no subset is defined.

---

## **Parameters**
1. **`dfspark (Spark DataFrame)`**
   - The input Spark DataFrame containing the data.

2. **`interval_minutes (int)`**
   - The frequency (in minutes) for interpolation.
   - Defines the time interval between interpolated points.

3. **`group (Optional[list of strings], default=None)`**
   - Specifies the columns by which data should be grouped for interpolation.
   - If no value is provided, the function **automatically groups by `imo`**.

4. **`columns (Optional[list of strings], default=["*"])`**
   - Defines the columns to retain in the output DataFrame.
   - If not specified, all columns from the input DataFrame are returned.

---

## **Return Value**
- **`df_interpolated (Spark DataFrame)`**
  A cleaned DataFrame containing interpolated data with the following characteristics:
  - Interpolated **latitude and longitude** at uniform intervals defined by `interval_minutes`.
  - Interpolation limited to a **maximum of 2 hours** between consecutive points.
  - Retains only the **specified columns** or all original columns if no subset is defined.

---

## **Internal Processing Flow**

### **1. Initial Setup**
- Computes the **resampling frequency** (`resample_interval`) in seconds from `interval_minutes`.
- If `columns` is not specified, all input DataFrame columns are considered.
- Defines a **partitioning clause** for grouping, either by `imo, segr, flag` (if present) or the user-defined `group` columns.

### **2. Time Limits Calculation**
- Computes **start (`PreviousTimestampRoundUp`)** and **end (`NextTimestampRoundDown`)** timestamps for each segment based on the defined interval.
- These limits are used to generate **uniform time sequences** within each group.

### **3. Linear Interpolation**
- Generates **evenly spaced interpolated points** within the calculated time limits.
- For each interpolated point:
  - **Latitude (`latitude`)**:
    - Uses **linear interpolation** between previous (`latitude_lag`) and current (`latitude_old`) values if the time difference **is less than 2 hours**.
    - If the difference **exceeds 2 hours**, keeps the **original `latitude_old`**.
  - **Longitude (`longitude`)**:
    - Follows the same process as latitude.

### **4. Frequency Resampling**
- Aligns interpolated data at uniform intervals (`interval_minutes`).
- Computes **time gaps (`freq`)** between consecutive points:
  - If the time gap **exceeds 2 hours**, sets `freq = 0`.

### **5. Data Cleaning & Output**
- Removes **temporary columns** used for internal calculations.
- Returns the final **interpolated DataFrame**, ready for further analysis or processing.

---

# `_spatial.py`

## **Function: `wkt_load`**

### **Functionality**
The `wkt_load` function transforms spatial data from **Point (lat, lon) format** into **Well-Known Text (WKT) format** and creates a **Spark DataFrame** with **geometry objects**.
This format is required for **spatial filtering** in **Apache Sedona**.
However, **H3 hexagons** are often more efficient for many spatial applications.

---

## **Parameters**
1. **`spark (SparkSession)`**
   - The active **Spark session** required for running queries and managing the DataFrame.

2. **`df (DataFrame)`**
   - A **Spark DataFrame** containing tabular data with a **WKT field** that will be transformed into a geometry object.

3. **`wkt_field (str, optional, default='WKT_geom')`**
   - The name of the column containing **WKT strings**.
   - This column is used to create the **geometry column**.

4. **`drop_wkt (bool, optional, default=True)`**
   - Determines whether the original **WKT field** should be removed after creating the geometry object:
     - **`True`**: Removes the original WKT field.
     - **`False`**: Keeps the original WKT field in the resulting DataFrame.

---

## **Return Value**
- **`geom_df (DataFrame)`**
  - A **Spark DataFrame** with a new column `geom`, containing geometry objects created from the **WKT field**.
  - If **`drop_wkt=True`**, the original WKT field is removed.

---

## **Internal Processing Flow**

### **1. Create a Temporary View**
- Registers the input DataFrame as a **temporary SQL view** (`x`), allowing **SQL queries** to be executed on it.

### **2. Convert WKT Field to Geometry**
- Uses the **`ST_GeomFromWKT`** function from **Apache Sedona** to convert the **WKT field** (`wkt_field`) into a **geometry object**.
- Stores the result in a **new column `geom`**.

### **3. Geometry Type Validation**
- Prints the **schema** of the resulting DataFrame and displays the **first record** to verify that geometry objects have been created correctly.
- Helps detect errors in the **input data**, such as **malformed geometries**.

### **4. Return Processed DataFrame**
- Returns the **processed Spark DataFrame**, including the newly created **geometry column (`geom`)**.

---

# `_vessel_specs_ghg4`

## Functions

### **1. `find_folder_csv(folder_name, file name)`**
**Functionality:**
Finds csv file within instructed folder (within the pacakge) and loads a Pandas DF

#### **Parameters:**
- **`folder name (str)`**: The input text as folder name
- **`file_name (str)`**: The input text as file name, including extension .csv.

#### **Return Value:**
- **`df (pd.DataFrame)`**: A dataframe with information stored at the package csv

---

### **2. `clean_string(text)`**
**Functionality:**
Removes punctuation marks from a text and converts it to lowercase.

#### **Parameters:**
- **`text (str)`**: The input text.

#### **Return Value:**
- **`str`**: The processed text, without punctuation and in lowercase.

---

### **3. `compare_similarity(text)`**
**Functionality:**
Compares the similarity between the input text and a list of standard vessel types. If the similarity is greater than **75%**, it returns the corresponding vessel type.

#### **Parameters:**
- **`text (str)`**: The input text to compare.

#### **Return Value:**
- **`str`**: The matching vessel type, or `None` if no match is found.

---

### **4. `bin_finder(vessel_t, value, df_in)`**
**Functionality:**
Classifies a vessel type into its respective **IMO bin** based on the ranges defined in the **IMO GHG4** report.

#### **Parameters:**
- **`vessel_t (str)`**: Vessel type.
- **`value (float)`**: Reference value for classification.
- **`df_in (pd.DataFrame)`**: Input DataFrame with classification ranges.

#### **Return Value:**
- **`int`**: IMO **bin number**, or `0` if no match is found.

---

## Specific Assignments

### **Cargo Units by Vessel Type**
Defines the standard cargo unit according to the vessel type:
- **Deadweight (DWT)** for bulk carriers, tankers, and general cargo vessels.
- **TEU** for container ships.
- **Gross Tonnage (GT)** for cruise ships, ferries, and other specialized vessels.

### **Engine Types by Energy Source**
Classifies vessels based on their **engine type and energy source**:
- **Oil Engines → Diesel, HFO**
- **Sails → Auxiliary wind energy**
- **Gas Turbines → High-speed propulsion**
- **Steam Turbines → Propulsion in specialized vessels**

---

### **1. `check_json(url)`**
**Functionality:**
Downloads and decodes JSON data from a specified URL.

#### **Parameters:**
- **`url (str)`**: The URL where the JSON file is retrieved.

#### **Return Value:**
- **`json_str (str)`**: A string containing the downloaded and decoded JSON content.

---

### **2. `clean_string(text)`**
**Functionality:**
Removes punctuation marks from a text and converts it to lowercase.

#### **Parameters:**
- **`text (str)`**: The input text.

#### **Return Value:**
- **`str`**: The cleaned text without punctuation and in lowercase.

---

### **3. `compare_similarity(text)`**
**Functionality:**
Compares the similarity between the input text and a list of **standard vessel types**. If the similarity is higher than **75%**, it returns the corresponding vessel type.

#### **Parameters:**
- **`text (str)`**: The input text to compare.

#### **Return Value:**
- **`str`**: The matching vessel type, or `None` if no match is found.

---

### **4. `bin_finder(vessel_t, value, df_in)`**
**Functionality:**
Classifies a vessel type into its respective **IMO bin** based on ranges defined in the **IMO GHG4 report**.

#### **Parameters:**
- **`vessel_t (str)`**: The vessel type.
- **`value (float)`**: The reference value for classification.
- **`df_in (pd.DataFrame)`**: The input DataFrame containing classification ranges.

#### **Return Value:**
- **`int`**: The **IMO bin number**, or `0` if no match is found.

---

### **5. Specific Assignments**

#### **Cargo Measurement Units per Vessel Type**
Defines the **standard cargo measurement unit** based on vessel type:
- **Deadweight (DWT)** for bulk carriers, tankers, and general cargo vessels.
- **TEU** for container ships.
- **Gross Tonnage (GT)** for cruise ships, ferries, and specialized vessels.

#### **Engine Type Allocation**
Classifies vessels by **engine type and energy source**:
- **Oil Engines → Diesel, HFO**
- **Sail → Wind-powered auxiliary propulsion**
- **Gas Turbines → High-speed propulsion**
- **Steam Turbines → Specialized vessel propulsion**


## **Function: `adapted_specs_imo`**

### **Functionality**
The `adapted_specs_imo` function adjusts vessel specifications according to the **IMO GHG4 report methods**.
If a vessel type is missing in the **Lloyd’s Fleet Register**, the function applies **cosine similarity** to find the closest match from a predefined list.
The result is a **Pandas DataFrame** with vessel specifications structured according to IMO GHG4 standards.

---

## **Parameters**
1. **`df_unique_imo (Pandas DataFrame)`**
   - Input DataFrame containing unique AIS records merged with **IHS Markit** vessel specifications.

---

## **Return Value**
- **`ind (Pandas DataFrame)`**
  A DataFrame with **AIS vessel specifications**, adjusted to IMO GHG4 methods, including the following additional columns:
  - **`imobin`**: IMO bin classification based on **IMO GHG4 recognized bins**.
  - **`fuel`**: Assigned fuel type for the vessel.
  - **`meType`**: Assigned main engine type.

---

## **Internal Processing Flow**

### **1. Initial Preparation**
- Renames columns for consistency (`vessel_type_main` → `ais_type`, etc.).
- Fills missing values in **`ShiptypeLevel5`** using AIS records.

### **2. Vessel Type Assignment**
- If **ShiptypeLevel5** does not match the standard vessel type list, **cosine similarity** is applied to find the closest match.
- Any vessel without a recognized **standard vessel type** is removed.

### **3. IMO Bin Classification**
- Merges the data with the **standard vessel type table**.
- Computes the **IMO bin** based on the vessel’s carrying capacity (**Deadweight, Gross Tonnage, TEU**).

### **4. Fuel Type Assignment**
- Determines the **fuel type** based on **propulsion type and IMO GHG4 category**.
- If the fuel type is unknown, assigns a default value based on the engine type (`HFO`, `MDO`, `LNG`, etc.).

### **5. Engine Type Assignment**
- Classifies the **engine type** based on its **propulsion and revolutions per minute (RPM)**.
- Assigns categories like **SSD (Slow Speed Diesel), MSD (Medium Speed Diesel), HSD (High Speed Diesel), Steam Turbine, Gas Turbine, Sail, Non-Propelled**, etc.

### **6. Final Validations and Return**
- Removes **intermediate processing columns**.
- Returns the **processed DataFrame**.

---

