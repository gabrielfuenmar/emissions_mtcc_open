"""
Created on Jan 14 2025

@author: Gabriel Fuentes
"""

import pandas as pd
import numpy as np
from pyproj import Proj
from numpy.linalg import inv
from pykalman import KalmanFilter

# Function to predict the state of an object (e.g., vessel) based on current position, velocity, acceleration, and time step
def prediction(x, y, vx, vy, dt, ax, ay):
    """
    Predicts the next state of an object using a simple motion model.

    Parameters:
    - x, y: Current position coordinates (float).
    - vx, vy: Current velocity components (float).
    - dt: Time step in seconds (float).
    - ax, ay: Acceleration components (float).

    Returns:
    - X_prime: Predicted state matrix (numpy array).
    """
    # State transition matrix
    A = np.array([[1, 0, dt, 0],
                  [0, 1, 0, dt],
                  [0, 0, 1, 0],
                  [0, 0, 0, 1]])

    # Current state vector
    X = np.array([[x],
                  [y],
                  [vx],
                  [vy]])

    # Control input matrix
    B = np.array([[0.5 * dt ** 2, 0, dt, 0],
                  [0, 0.5 * dt ** 2, 0, dt],
                  [0, 0, dt, 0],
                  [0, 0, 0, dt]])

    # Control vector (acceleration)
    a = np.array([[ax],
                  [ay],
                  [ax],
                  [ay]])

    # Predicted state with no noise included. 
    ##No noise in process that I could tell. Potentially the difference on heading and course here If fed by heading   
    X_prime = A.dot(X) + B.dot(a)

    return X_prime

# Function to compute a covariance matrix
def covariance(sigma1, sigma2, sigma3, sigma4):
    """
    Computes the covariance matrix for a multivariate normal distribution.

    Parameters:
    - sigma1, sigma2, sigma3, sigma4: Standard deviations for each variable (float).

    Returns:
    - cov_matrix: Covariance matrix (numpy array).
    """
    # Compute pairwise covariances
    cov_matrix = np.array([[sigma1 ** 2, sigma1 * sigma2, sigma1 * sigma3, sigma1 * sigma4],
                           [sigma2 * sigma1, sigma2 ** 2, sigma2 * sigma3, sigma2 * sigma4],
                           [sigma3 * sigma1, sigma3 * sigma2, sigma3 ** 2, sigma3 * sigma4],
                           [sigma4 * sigma1, sigma4 * sigma2, sigma4 * sigma3, sigma4 ** 2]])

    return cov_matrix

# Function to extrapolate and smooth vessel data using Kalman Filter
def extrapolate(sample, freq=10):
    """
    Extrapolates vessel trajectory data and smoothens it using a Kalman Filter.

    Parameters:
    - sample: DataFrame containing vessel data with columns including:
        * dt_pos_utc: Datetime of position updates.
        * latitude, longitude: Geographic coordinates (float).
        * vx, vy: Velocity components (float).
        * ax, ay: Acceleration components (float).
        * sog: Speed over ground (float).
        * cog: Course over ground (float).
        * Additional vessel metadata columns.
    - freq: Resampling frequency in minutes (int).

    Returns:
    - sample: Processed and extrapolated DataFrame with smoothed positions, velocities, and metadata.
    """
    start = sample.copy()
    columns_align = start.columns
    
    # Check if the vessel is in an anchoring group or port group
    if sample.anchoring_group.iloc[0] == 1 or sample.port_group.iloc[0] == 1:
        try:
            cols = sample.columns.tolist()
            
            # Set up the projection (UTM Zone 17N)
            proj = Proj("+proj=utm +zone=17 +north +ellps=WGS84 +datum=WGS84 +units=m +no_defs")

            # Constants
            dt = 60  # Time step in seconds
            ns1min = 60 * 1e9  # One minute in nanoseconds

            # Convert datetime to uniform intervals
            sample = sample.assign(dt_pos_utc=pd.to_datetime(sample.dt_pos_utc))
            sample["dt_pos_utc"] = pd.to_datetime((sample["dt_pos_utc"].astype(np.int64) // ns1min + 1) * ns1min)
            sample.drop_duplicates(subset=["dt_pos_utc"], inplace=True)

            # Convert SOG from knots to meters per second
            sample = sample.assign(sog=sample.sog * 0.514444)

            # Transform coordinates to UTM
            X, Y = proj(sample.longitude.values, sample.latitude.values)
            sample = sample.assign(X=X, Y=Y)

            # Predict future positions
            sol = pd.DataFrame(
                np.concatenate(np.vectorize(prediction, otypes=[np.ndarray])(
                    sample.X, sample.Y, sample.vx, sample.vy, sample.dt, sample.ax, sample.ay)
                ).reshape(-1, 4),
                columns=["X_est", "Y_est", "vx_est", "vy_est"]
            )
            
            # Combine predictions with original data
            sample.reset_index(drop=True, inplace=True)
            sample = pd.concat([sample, sol.shift()], axis=1)

            # Compute estimation errors
            sample = sample.assign(
                X_error=sample.X - sample.X_est,
                Y_error=sample.Y - sample.Y_est,
                vx_error=sample.vx - sample.vx_est,
                vy_error=sample.vy - sample.vy_est
            )

            # Error covariance matrix for process model
            error_est_X = sample.X_error.std()
            error_est_Y = sample.Y_error.std()
            error_est_vx = sample.vx_error.std()
            error_est_vy = sample.vy_error.std()
            P = covariance(error_est_X, error_est_Y, error_est_vx, error_est_vy)

            # Kalman Filter setup
            A = np.array([[1, 0, dt, 0],
                          [0, 1, 0, dt],
                          [0, 0, 1, 0],
                          [0, 0, 0, 1]])

            ###Measurement errors
            error_obs_x=0.713 ##As per NCOSBP User Range Error
            error_obs_y=0.713
            error_obs_vx=0.05 ##0.18 km h as Al-Gaadi 2005.
            error_obs_vy=0.05

            R = covariance(error_obs_x, error_obs_y, error_obs_vx, error_obs_vy)  # Observation errors
            observations = np.ma.masked_invalid(sample[["X", "Y", "vx", "vy"]].values)
            obs_acc = np.c_[sample.ax.values, sample.ay.values, sample.ax.values, sample.ay.values]

            H = np.identity(4)
            X_init = observations[0]

            kf = KalmanFilter(
                transition_matrices=A,
                observation_covariance=R,
                initial_state_covariance=P,
                initial_state_mean=X_init,
                observation_matrices=H
            )

            # Smooth the observations
            smoothed_state_means, smoothed_state_covariances = kf.smooth(observations)

            # Convert UTM coordinates back to lat/lon
            lon, lat = proj(smoothed_state_means[:, 0], smoothed_state_means[:, 1], inverse=True)

            # Update sample with smoothed values
            sample = sample.assign(
                longitude=lon, latitude=lat,
                vx=smoothed_state_means[:, 2], vy=smoothed_state_means[:, 3],
                sog=np.where(sample.sog.isnull(), ((sample.vx ** 2 + sample.vy ** 2) ** 0.5).round(4), sample.sog),
                cog=np.where(sample.cog.isnull(), np.mod((np.arctan2(sample.vx, sample.vy) * 180 / np.pi) + 180, 360), sample.cog)
            )

            # Finalize metadata and resampling
            sample = sample[cols]
            sample = sample.resample(f"{freq}T").asfreq().reset_index()

        except Exception as e:
            print(f"Error processing sample: {e}")
            sample = start
    
    return sample


