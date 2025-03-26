# **Efficiency Functions**

## **Kalmar Module (`kalmar.py`)**

### **1. `prediction(x, y, vx, vy, dt, ax, ay)`**
**Functionality:**
Predicts the future position and velocity of an object in a two-dimensional system using a kinematic model.

#### **Parameters:**
- `x (float)`: Current position on the X-axis (longitude).
- `y (float)`: Current position on the Y-axis (latitude).
- `vx (float)`: Current velocity on the X-axis.
- `vy (float)`: Current velocity on the Y-axis.
- `dt (float)`: Time interval for prediction.
- `ax (float)`: Acceleration on the X-axis.
- `ay (float)`: Acceleration on the Y-axis.

#### **Return Value:**
- `X_prime (numpy.ndarray)`: A **4×1** matrix containing the predicted positions and velocities in X and Y.

---

### **2. `covariance(sigma1, sigma2, sigma3, sigma4)`**
**Functionality:**
Creates a covariance matrix based on the standard deviations (**sigma**) of different variables. The variance models expected measurement errors in the equipment transmitting the information.

#### **Parameters:**
- `sigma1 (float)`: Standard deviation of the first variable.
- `sigma2 (float)`: Standard deviation of the second variable.
- `sigma3 (float)`: Standard deviation of the third variable.
- `sigma4 (float)`: Standard deviation of the fourth variable.

#### **Return Value:**
- `cov_matrix (numpy.ndarray)`: A **4×4** covariance matrix where:
  - The **diagonal values** represent the variances (**sigma²**) of the variables.
  - The **off-diagonal values** represent the covariances between the variables.

---
### **3. `extrapolate(sample, freq=10)`**
**Functionality:**
The function performs the following steps:
1. Converts geographic positions (latitude and longitude) to Cartesian coordinates in the UTM system.
2. Estimates positions and velocities using an iterative prediction approach.
3. Evaluates estimation errors and applies a **Kalman filter** to smooth the data.
4. Reconstructs the data in geographic terms and adds missing information, such as **Speed Over Ground (SOG)** and **Course Over Ground (COG)**.
5. Creates a new dataset with uniformly sampled data and returns the refined result.

#### **Parameters:**
- `sample (DataFrame)`: An input dataset containing positional, velocity, and other relevant variables for a moving object (e.g., vessels). The expected columns include:
  - `longitude (float)`: Longitude in degrees.
  - `latitude (float)`: Latitude in degrees.
  - `sog (float)`: Speed Over Ground.
  - `vx, vy (float)`: Velocity components in the X and Y axes, respectively.
  - `ax, ay (float)`: Acceleration components in the X and Y axes, respectively.
  - `dt_pos_utc (datetime)`: Timestamp associated with each record.
  - Other metadata, such as **IMO, MMSI**, etc.
- `freq (int, optional)`: Sampling frequency in minutes for data extrapolation (default is **10 minutes**).

#### **Return Value:**
- `sample (DataFrame)`: A cleaned dataset containing processed and smoothed data with the following characteristics:
  - Data interpolated at a constant frequency specified by `freq`.
  - Additional estimated and smoothed variables, such as **longitude, latitude, sog, and cog**.
  - Restored and reorganized original columns, preserving the initial dataset format.

---

### **Internal Processing Flow:**

#### **1. Initial Preparation:**
- The function checks whether the columns `anchoring_group` or `port_group` have a value of **1** to process the data.
- Original columns are copied to maintain the initial structure.

#### **2. Conversion to UTM Coordinates:**
- The **pyproj (Proj)** library is used to transform **latitude and longitude** into **UTM Cartesian coordinates**, facilitating accurate distance and velocity calculations.

#### **3. Initial Prediction:**
- An initial prediction is computed for positions and velocities based on known **accelerations (`ax, ay`)** and **velocities (`vx, vy`)**.
- Prediction errors in coordinates and velocities are estimated.

#### **4. Covariance Matrix Calculation:**
- Errors in positions (**X, Y**) and velocities (**vx, vy**) are evaluated and used to construct the **covariance matrix `P`**.
- A second covariance matrix **`R`** models measurement errors based on constant values.

#### **5. Kalman Filter Application:**
- A **Kalman filter** is initialized with the **transition, observation, and covariance matrices**.
- An iterative smoothing process adjusts observed positions and velocities, minimizing errors.

#### **6. Inverse Transformation:**
- The smoothed **UTM coordinates** are transformed back into **latitude and longitude** to retain the original geographic format.

#### **7. Interpolation and Final Structuring:**
- Data is interpolated at a uniform frequency using **pandas**.
- The original columns are restored, and intermediate calculated variables are removed.
- Missing values in some columns are **forward-filled (`ffill`)** to complete the DataFrame.

---
