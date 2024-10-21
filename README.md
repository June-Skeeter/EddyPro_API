# EddyPro API

Written and maintained by Dr. June Skeeter under an open copyright license&copy;.

# About

[EddyPro&reg;](https://www.licor.com/env/support/EddyPro/topics/introduction.html#top) is a popular software application for processing high frequency Eddy Covariance (EC) data via a graphic user interface (GUI).  This application program interface (API) can be used to automate and parallelize processing with Eddy Pro&reg; to mininmize computation times.

## Workflow

There are two main things the API does.

1. **Pre-processing**: finds the data within the source directory, parses .  

    a. Generate .csv files of biomet data and dynamic metadata as needed (**for Biomet.Net database users**).
    b. Search the source directory for all files in specified format (e.g., .ghg, .dat, etc.) covering the desired time period.
    c. Read all relevant data and embedded metadata (.ghg files) and generate descriptive statistics for each interval.
        * If embedded metadata are not available, the user can provide a template .metadata file to serve as a basis for processing.  This file can be created using EddyPro or a text editor.  
            * See **Templates/CR1000_LI7500_Template.metadata** for an example.  This .metadata file is for a simple setup consisting of a CSAT3 and a LI75000 logged at 10Hz on a CR1000.
    d. Filter data to exclude intervals with poor quality data that would prohibit reliable flux estimation. e.g., we can exclude intervals with mean flow rate <10 lpm for a LI-2700.
        * See **config_files/config.yml**: monitoringInstructions>dataFilters for a full list of filters or to add more of your own
    e. Overwrite metadata (embedded or template) with time varying metadata from a .csv file.
        * See **Templates/Manual_Metadata_Updates.csv** for an example
            * The file defines when settings were changed using TIMESTAMP Start and TIMESTAMP End (optional).  If an end is not provided, the change is assumed permanent, until another change is defined in the overwrite file.  If an end is provided the change only applies to the time periods between (inclusive of) the Start and End TIMESTAMP.  Only metadata that were changed should be defined for a given record.  Records are specified in two column headers corresponding to their section and key values in a LICOR .metadata file.
    f. Group the data by periods of common configurations, so EddyPro will only be run on periods with equivalent settings.
        * e.g., if sensor separation changed, the data before and after the change would be run as separate groups for the purpose of generating representative spectral corrections


2. **Running EddyPro&reg**; takes the results of the pre-processing routine and uses it to generate and execute batch runs of EddyPro in parallel.  The outputs are then stitched together into homogenous files and can be output into binary Biomet.Net database format if desired.

# Installation & Setup

For now, the API will **only** work on windows.  There are plans to expand to mac/linux ... eventually.

1. [Install EddyPro](https://www.licor.com/env/support/EddyPro/software.html).  Make not of where the root installation of EddyPro ends up (e.g., C:/Program Files/LI-COR/EddyPro-7.0.9/) as you will need this later

2. Clone the repository

```{bash}
git clone --recurse-submodules https://github.com/CANFLUX/EddyPro_API
```

3. Run the installation routine 

```{bash}
cd Path\To\eddyProAPI
python pyDbTools\setupPyVenv\install.py
```

<!-- 2. Create the virtual environment and install dependencies

* If you're using VSCode, it should autodetect the requirements.txt dependency list.  Open the EddyPro_API folder in VSCode.   Then hit ctrl+shift+p > type "Python" and select "Create Environment" then select requirements.txt when prompted.

* If installing via command line:
    
    a. CD into the EddyPro_API folder

    `CD /Path/to/EddyPro_API`
    
    b. To create the .venv type the following command.  *Note* if "py" doesn't work, try python, python, py3, python3 etc.  It will depend on how python is named on your system

    `py -m venv .venv`

    c. Activate the virtual environment.  *Note* if you are on a system which restricts the execution of scripts, first you need to temporarily set your execution policy in order to activate the environment: `Set-ExecutionPolicy Unrestricted -Scope Process`

    `.\.venv\Scripts\activate`

    d. Install the dependencies

    `pip install -r requirements.txt`

3. Make a copy of **config_files/user_path_definitions_template.yml** and name it **config_files\user_path_definitions.yml** then update the paths accordingly.

* The API needs to be pointed to you base installation of EddyPro.  Make sure you are running v7.0.9
* The API also needs you to define a working directory where outputs should be saved. If you were to set `workingDir: C:/highfreq/` then you would end up with outputs stored in  C:/highfreq/siteID/metadata and C:/highfreq/siteID/eddyProAPIOutputs -->

# Running the API

The API can be called via command line; instructions are given below.  You can also run it vai API_Run.ipynb if you prefer.

1. Before running EddyPro, you should decide which settings you want to use.  You can select an appropriate .eddypro file from the Templates folder or use an existing .eddypro file form previous work.

* The template files can be used to define the processing parameters and ensure they remain standardized between runs.  They can be inspected using the EddyPro GUI or a text editor.  It is suggested to use 'ClosedPathStandard.eddypro' for systems with an closed path analyzers (e.g., LI-7200) and 'OpenPathStandard.eddypro' for systems without any close path analyzers.  You can also define your own template file by modifying a template in the GUI or with a text editor and saving with a new filename name.
    * How will these files be used?  The API will read the settings from these files and then dynamically update settings in EddyPro (paths, dates, etc.) to automate runs.  Settings that will be overwritten can be found in **config_files/eddyProDynamicConfig.ini**

2. Call the API

    a. Activate the virtual environment.  *Note* if you are on a system which restricts the execution of scripts, first you need to temporarily set your execution policy in order to activate the environment: `Set-ExecutionPolicy Unrestricted -Scope Process`

    `.\.venv\Scripts\activate`

    b. Call the program.  In this example, the program will execute for the current year, on the "BB" site, using Templates/ClosedPathStandard.eddypro to define the processing procedures.

    `py eddyProAPI.py --siteID BB --eddyProStaticConfig Templates/ClosedPathStandard.eddypro`
    
