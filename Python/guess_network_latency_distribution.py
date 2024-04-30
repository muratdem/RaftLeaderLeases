"""Standalone script, find the params for a network latency probability distribution.

Find a lognormal distribution that matches Table 1 in Cloudy Forecast: How Predictable
is Communication Latency in the Cloud?, Hilyard et. al. 2023.
"""
import numpy as np
from scipy.stats import lognorm
from scipy.optimize import minimize

# The paper shows round-trip latencies, assume one-way is half (and accepting that this
# assumption adds some error).
aws_same_subnet = {
    "mean": 283 / 2,
    "median": 270 / 2,
    "5th": 215 / 2,
    "25th": 245 / 2,
    "90th": 325 / 2,
    "95th": 345 / 2,
}


# Function to calculate lognormal percentiles based on mu and sigma.
def lognormal_percentiles(mu, sigma):
    dist = lognorm(s=sigma, scale=np.exp(mu))
    return {
        "mean": dist.mean(),
        "median": dist.median(),
        "5th": dist.ppf(0.05),
        "25th": dist.ppf(0.25),
        "90th": dist.ppf(0.90),
        "95th": dist.ppf(0.95),
    }


# Objective function to minimize.
def objective(params):
    mu, sigma = params
    calculated = lognormal_percentiles(mu, sigma)
    error = 0
    # Sum of squared differences between observed and calculated percentiles.
    for key in aws_same_subnet:
        error += (aws_same_subnet[key] - calculated[key]) ** 2
    return error


# Initial guesses for mu and sigma.
initial_guess = [np.log(100), 1]  # These are arbitrary and might need adjusting

# Minimize the objective function.
result = minimize(objective, initial_guess, bounds=[(None, None), (0.0001, None)])

# Extract the optimal mu and sigma
mu, sigma = result.x
print(f"Optimal mu: {mu}, Optimal sigma: {sigma},"
      f" variance: {lognorm(s=sigma, scale=np.exp(mu)).var()}")
