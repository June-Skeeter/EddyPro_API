# EddyPro API

Created by Dr. June Skeeter

This project is still under development.

# About

This API is intended to streamline flux estimation using Eddy Pro.  The API allows you to run EddyPro in parallel, high-priority batches to mininmize computation times.  Preliminary testing indicates the EddyPro API can speed up processing by 67% compared using the EddyPro GUI.

## Intro

Eddy Covariance (EC) is a method for measuring ecosystem-atmosphere fluxes of energy and trace gasses (e.g., CO_2).  Flux esimation via EC involves applying a Reynolds Opperators to high frequency (>=10 Hz) measurements of scalar conentrations and 3D winds over 30- to 60-minute intervals.  This is a computationally expensive procedure, and when it must be repeated over months or years of data, it quickly becomes an incredibly time consuming procedure.

One of the most popular applications for processing EC data is EddyPro (LICOR).  "EddyPro is designed to provide easy, accurate EC flux computations." [see](https://www.licor.com/env/support/EddyPro/topics/introduction.html#top)  The application has a graphic user interface (GUI) that allows apply a standard processing pipeline or selectively apply more advanced filtering and correction procedures as needed.  The backend of the processing routine is then executed in FORTRAN, and eddypro outputs fluxes estimates and supplemental data as .csv files.

The EddyPro API (Application Program Interfae) is a python wrapper for the EddyPro GUI that allows for automation of flux processing procedures defined usising the EddyPro GUI, and more rapid processing of EC data via paralell processing.

## For Winmet

```
Set-ExecutionPolicy Unrestricted -Scope Process
.\.venv\Scripts\activate
```

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
* Update naming conventions and handling of output files to be more organized and prevent overwriting outputs
* Put metadata update files in more sensible (site specific) location
* Time lag optimization
* Cospectral analysis