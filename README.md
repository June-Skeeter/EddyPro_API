# EddyPro API

Created by Dr. June Skeeter

This project is still under development.

## About

This API is intended to streamline flux estimation using Eddy Pro.  The API allows you to run EddyPro in parallel batches to speed up processing.  Preliminary testing indicates the EddyPro API can speed up processing by 67% compared using the EddyPro GUI


## Workflow


### Preprocessing


1. Read high frequency data in .ghg or .dat format.

* If .ghg data are provided extract the data and read relevant sub-files
* Calculate raw half-hourly means of all input data
* Parse and track the metadata, inspecting for changes
    * Update/correct metadata from a user-defined template as necessary

2. Create metadata template files

* Group the metadata into blocks of time with uniform (or acceptably similar) settings
* For each such period create a .metadata file and .eddypro file with time-period specific settings.

## Part 2: Running Eddypro

Create a .eddypro template, either using the GUI, or editing one of the templates included here.  Then call EddyPro to run over a given time period - the API will handle updating the time period specific settings and partitioning the outputs by relevant period.  Where necessary, the API will separate cospectral inputs/outputs by accordingly.
