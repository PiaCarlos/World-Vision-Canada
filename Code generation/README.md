# Automatic Overlap Code Generator

Manually assigning overlap codes to projects can be slow, error-prone, and time-consuming. This Python script automates the process, ensuring accurate coding and reliable calculation of total project reach.

## Features

- **Automatic grouping by location:** Projects sharing the same geographic area get the same overlap code, preventing double-counting of beneficiaries.  
- **Handles complex overlaps:** Projects spanning multiple locations are correctly accounted for.  
- **Maintains data integrity:** Existing "unique" codes are preserved. Special program projects (e.g., GIK and WFP) get distinct codes.  
- **Systematic code assignment:** Codes are based on country and overlap group, e.g., `OV_BOLIVIA_1`. Projects without a specific region receive a default code like `OV_BOLIVIA_0`.  

## Benefits

- Eliminates manual coding errors  
- Saves time and effort  
- Provides consistent and accurate project overlap tracking  

## How it works

The script connects to your database and generates overlap codes automatically according to the rules above, making the calculation of total beneficiary reach efficient and reliable. This was implemented into WVC workflow. this is a modified version with the names of the databases hidden and not any private WVC information. 
