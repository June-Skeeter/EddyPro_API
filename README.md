# EddyPro API

Created by Dr. June Skeeter

This project is still under development.

## About

This API is intended to streamline flux estimation using Eddy Pro.  The API allows you to run EddyPro in parallel batches to speed up processing.  Preliminary testing indicates the EddyPro API can speed up processing by 67% compared using the EddyPro GUI

## For Winmet

> Set-ExecutionPolicy Unrestricted -Scope Process
> PS C:\Users\labuser\EddyPro_API\.venv\Scripts> .\Activate.ps1

## Workflow


### Preprocessing

1. Read high frequency data in .ghg or .dat (not yet done) format.

Example call from command line:

```

py .\preProcessing.py --SiteID BB --Years 2022 --Processes 4

```

* This will parse all data, month by month for the BB site in the highfreq folder using four cores to speed up processing time.

* **If .ghg data** 
    * Extract the data and read relevant sub-files
    * Calculate raw half-hourly means of all input data
    * Parse and track the metadata, inspecting for changes
        * Update/correct metadata from a user-defined template as necessary

* **If other .data files**

2. Create metadata template files


Example call from command line:

```
py .\setupEP.py --SiteID BB --Template ep_templates/LabStandard_Advanced.eddypro --RunDates "2022-07-01 00:00" "2022-07-31 23:59" 

```

* This will calculate fluxes for BB over July, 2022 using the lab standard template

* Group the metadata into blocks of time with uniform (or acceptably similar) settings
* For each such period create a .metadata file and .eddypro file with time-period specific settings.

### Running Eddypro

Create a .eddypro template, either using the GUI, editing one of the templates included here.  Then call EddyPro to run over a given time period - the API will handle updating the time period specific settings and partitioning the outputs by relevant period.  Where necessary, the API will separate cospectral inputs/outputs by accordingly.



## Pending Tasks

* Enhance documentation
* Procedures for updating slices of preprocessing without re-running whole batches
* Concatenate output files
* Update naming conventions and handling of output files to be more organized and prevent overwriting outputs
* Put metadata update files in more sensible (site specific) location
* Time lag optimization
* Cospectral analysis
* Setup to use .dat files as well