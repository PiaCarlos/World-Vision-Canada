# Legacy Code Translation & Optimization: Azure → Snowflake

This project demonstrates the translation and optimization of a **3,000+ line legacy codebase** from Azure to Snowflake, including major efficiency improvements and refactoring.

## Overview

- Translated legacy code to Snowflake, making all necessary adjustments for proper functionality.  
- Refactored and simplified many sections to improve readability and maintainability.  
- **Boosted performance:** Reduced execution time from **31 minutes to 2 minutes** (>90% improvement).  
- **Cost savings:** Optimizations contributed to saving over **$1,000 per month**.  

## Key Features

- Converted slow row-wise operations (`pandas apply`) into **highly efficient vectorized code** using **NumPy**.  
- Ensured correctness and preserved the original logic while improving efficiency.  

## Example Functions

In this folder there is an example of two important functions that were optimized using vectorization
