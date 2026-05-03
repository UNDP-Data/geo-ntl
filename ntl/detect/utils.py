import numpy as np
from scipy.ndimage import label

def ransac(x=None, y=None, iterations=100, threshold=5.0):
    """
    Calculates RANSAC linear regression using only standard NumPy.
    """
    best_inliers = 0
    best_slope = 0.0
    best_intercept = 0.0
    n = len(x)

    for _ in range(iterations):
        # 1. Randomly pick 2 points to define a line
        idx1, idx2 = np.random.randint(0, n, 2)
        if x[idx1] == x[idx2]:
            continue  # Avoid division by zero

        # 2. Calculate the line equation (y = mx + c)
        m = (y[idx2] - y[idx1]) / (x[idx2] - x[idx1])
        c = y[idx1] - m * x[idx1]

        # 3. Vectorized residual calculation (This is where NumPy shines)
        predictions = m * x + c
        residuals = np.abs(y - predictions)

        # 4. Count the inliers
        inlier_count = np.sum(residuals < threshold)

        # 5. Keep the best model
        if inlier_count > best_inliers:
            best_inliers = inlier_count
            best_slope = m
            best_intercept = c

    return best_slope, best_intercept

def calculate_dynamic_threshold(baseline_pixels=None, raw_pixels=None , multiplier=3.0):
    """
    Calculates a data-driven RANSAC threshold using MAD.
    """
    # 1. Find the raw median difference (immune to dark outages)
    median_shift = np.median(baseline_pixels - raw_pixels)

    # 2. Calculate how far typical pixels deviate from that median
    residuals = np.abs((baseline_pixels - raw_pixels) - median_shift)

    # 3. Calculate the Median Absolute Deviation (MAD)
    mad = np.median(residuals)

    # 4. Convert MAD to a robust Standard Deviation (constant is 1.4826)
    robust_std = 1.4826 * mad

    # 5. Set threshold to N standard deviations (3.0 captures 99.7% of normal noise)
    dynamic_threshold = multiplier * robust_std

    # Safety rail: Establish an absolute minimum noise floor for perfectly clear nights
    #return max(dynamic_threshold, .75)
    return dynamic_threshold




def spatial_filter(outage_map, min_size=2):
    # 1. Group connected pixels into "clumps"
    labeled_array, num_features = label(outage_map)

    # 2. Count how many pixels are in each clump
    clump_sizes = np.bincount(labeled_array.ravel())

    # 3. Create a mask of clumps that meet your size requirement
    mask_size = clump_sizes >= min_size

    # 4. Filter the original map (clump 0 is the background, so we ignore it)
    mask_size[0] = 0
    return mask_size[labeled_array]


def calculate_qf_multiplier(qf1_patch, cmask_patch, lunar_illum, base_k=1.5):
    """
    Adjusts the RANSAC multiplier based on pixel quality.
    """
    penalty = 0.0

    # 1. Stray Light Penalty
    # If more than 10% of the patch is stray-light affected, increase multiplier
    stray_ratio = np.mean((qf1_patch & 32) > 0)
    penalty += (stray_ratio * 2.0)  # Up to +2.0 penalty

    # 2. Cloud Confidence Penalty
    # If the mask is 'Probably Clear' (1) vs 'Confidently Clear' (0)
    prob_clear_ratio = np.mean(cmask_patch == 1)
    penalty += (prob_clear_ratio * 1.0)  # Up to +1.0 penalty

    # 3. Lunar Glint Penalty
    # Moon illumination is a float 0.0 to 1.0
    penalty += (lunar_illum * 0.5)

    return base_k + penalty