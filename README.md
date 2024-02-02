# EddyPro API

Created by Dr. June Skeeter

This project is still under development.

## About

This API is intended to streamline flux estimation using Eddy Pro.  The API allows you to run EddyPro in parallel batches to speed up processing.  Preliminary testing indicates the EddyPro API can speed up processing by 67% compared using the EddyPro GUI


## Workflow


### Preprocessing

1. Read high frequency data in .ghg or .data format.

* **If .ghg data** 
    * Extract the data and read relevant sub-files
    * Calculate raw half-hourly means of all input data
    * Parse and track the metadata, inspecting for changes
        * Update/correct metadata from a user-defined template as necessary

* **If other .data files**

2. Create metadata template files

* Group the metadata into blocks of time with uniform (or acceptably similar) settings
* For each such period create a .metadata file and .eddypro file with time-period specific settings.

### Running Eddypro

Create a .eddypro template, either using the GUI, editing one of the templates included here.  Then call EddyPro to run over a given time period - the API will handle updating the time period specific settings and partitioning the outputs by relevant period.  Where necessary, the API will separate cospectral inputs/outputs by accordingly.



## Pending Tasks

* Enhance documentation
* Update naming conventions and handling of output files to be more organized and prevent overwriting outputs
* Put metadata update files in more sensible (site specific) location
* Time lag optimization
* Cospectral analysis
* Setup to use .dat files as well