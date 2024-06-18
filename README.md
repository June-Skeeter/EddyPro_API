# EddyPro API

Created by Dr. June Skeeter under an open copyright license.

This project is still under development.

# About

This API is intended to streamline flux estimation using Eddy Pro.  The API allows you to run EddyPro in parallel, high-priority batches to mininmize computation times.  Preliminary testing indicates the EddyPro API can speed up processing by 67% compared using the EddyPro GUI.

## Intro

Eddy Covariance (EC) is a method for measuring ecosystem-atmosphere fluxes of energy and trace gasses (e.g., CO_2).  Flux esimation via EC involves applying a Reynolds Opperators to high frequency (>=10 Hz) measurements of scalar conentrations and 3D winds over 30- to 60-minute intervals.  This is a computationally expensive procedure, and when it must be repeated over months or years of data, it quickly becomes an incredibly time consuming procedure.

One of the most popular applications for processing EC data is EddyPro (LICOR).  "EddyPro is designed to provide easy, accurate EC flux computations." [see](https://www.licor.com/env/support/EddyPro/topics/introduction.html#top)  The application has a graphic user interface (GUI) that allows apply a standard processing pipeline or selectively apply more advanced filtering and correction procedures as needed.  The backend of the processing routine is then executed in FORTRAN, and eddypro outputs fluxes estimates and supplemental data as .csv files.

The EddyPro API (Application Program Interfae) is a python wrapper for the EddyPro GUI that allows for automation of flux processing procedures defined usising the EddyPro GUI, and more rapid processing of EC data via parallel processing.

# Installation

## For a Personal Computer

This will **only** work on windows for now, there are plans to expand to mac/linux ... eventually.

1. Create the virtual environment and install dependencies

* If you're using VSCode, it should autodetect the requirements.txt dependency list.  Open the EddyPro_API folder in VSCode.   Then hit ctrl+shift+p > type "Python" and select "Create Environment" then select requirements.txt when prompted.

* If installing via command line:
    
    a. CD into the EddyPro_API folder

    `CD /Path/to/EddyPro_API`
    
    b. To create the .venv type the following command.  *Note* if "python3" doesn't work, try python, py, py3 etc.  It will depend on how python is named on your system

    `python3 -m venv .venv`

    c. Activate the virtual environment

    `.\.venv\Scripts\activate`

    d. Install the dependencies

    `pip install -r requirements.txt`

## For Winmet

* Open the EddyPro_AIP in VSCode then open the powershell console.  Winmet requires you to temporarily override some settings to get things to work:

```
Set-ExecutionPolicy Unrestricted -Scope Process
.\.venv\Scripts\activate
```


## Workflow

For now, stick to using the jupyter notebook *API_Run.ipynb*, command line instructions will be mad available soon.

### Setup

* If you're on WinMet, it should already be setup, but check config_files\user_path_definitions.yml to make sure things are pointing in the right direction.

* If you're on a personal computer, make a copy of config_files/user_path_definitions_template.yml and name it config_files\user_path_definitions.yml then define the paths accordingly.

### Preprocessing

1. Create the "auxillary data", which includes a biomet.CSV file and a dynamicMetadata.csv file.  If you're a Biomet.Net user, you can read these directly from the database.

2. Read high frequency data in .ghg or .dat format.  If you're using .ghg files, it should handle everything automatically unless there are metadata settings you need to override.  If you're using .dat files, you'll need to provide one or more (if settings change drastically) .metadata files in LICOR's metadata format.  You can use the EddyPro GUI to set one up, or use a template (to be added later).

### Running Eddypro

Use one of the EddyPro Templates provided, then overwrite any settings you want with a user defined dict or yaml file with the following format:

`{section:{key:value}}`

Run the API over you desired dates then inspec the output.

## Pending Tasks

* Enhance documentation
* Update naming conventions and handling of output files to be more organized and prevent overwriting outputs
* Put metadata update files in more sensible (site specific) location
