# EddyPro Automation

A collection of Python code that can be used to run EddyPro.

## Workflow


### Preprocessing


1. Read high frequency data.  Calculate raw means, uncorrected covariances, and track flags.

2. Create metadata templates

* Inspect for changes in setup/configuration to identify blocks of time that are similar.

    * **.ghg files**:
        * Create batches of .metadata templates
        * User provides dynamic, time-variable metadata

    * **.dat files:**:
        * User provides .metadata template
        * User provides dynamic, time-variable metadata

## Part 2: Running Eddypro

Generate ini files for .ghg data and create batches of similar files (e.g. same calibrations, season, sensor separation, etc.) for submitting recalc runs to eddypro


These are all the values contained in the .METADATA, .DATA, .STATUS, system_config/co2app.conf files.

## Metadata Values

More info [here](https://www.licor.com/env/support/EddyPro/topics/metadata-file-editor.html)

Site
['site_name', 'altitude', 'latitude', 'longitude', 'canopy_height', 'displacement_height', 'roughness_length']

Station
['station_name', 'logger_id', 'logger_sw_version']

Timing
['acquisition_frequency', 'file_duration']

Instruments
['instr_1_manufacturer', 'instr_1_model', 'instr_1_sn', 'instr_1_sw_version', 'instr_1_id', 'instr_1_height', 'instr_1_wformat', 'instr_1_wref', 'instr_1_north_offset', 'instr_1_head_corr', 'instr_1_northward_separation', 'instr_1_eastward_separation', 'instr_1_vertical_separation', 'instr_2_manufacturer', 'instr_2_model', 'instr_2_sn', 'instr_2_id', 'instr_2_sw_version', 'instr_2_tube_length', 'instr_2_tube_diameter', 'instr_2_tube_flowrate', 'instr_2_northward_separation', 'instr_2_eastward_separation', 'instr_2_vertical_separation', 'instr_3_manufacturer', 'instr_3_model', 'instr_3_sn', 'instr_3_id', 'instr_3_sw_version', 'instr_3_tube_length', 'instr_3_tube_diameter', 'instr_3_tube_flowrate', 'instr_3_northward_separation', 'instr_3_eastward_separation', 'instr_3_vertical_separation']

FileDescription
['separator', 'flag_discards_if_above', 'header_rows', 'data_label', 'col_1_variable', 'col_1_instrument', 'col_1_measure_type', 'col_1_unit_in', 'col_1_conversion', 'col_1_min_value', 'col_1_max_value', 'col_1_unit_out', 'col_1_a_value', 'col_1_b_value', 'col_1_nom_timelag', 'col_1_min_timelag', 'col_1_max_timelag', ... this pattern then repeats for every subsequent column.]

## Data Values
More info on the data file [here](https://www.licor.com/env/support/LI-7200RS/topics/data-files.html); info on 7200/CSAT diagnositcs [here](https://www.licor.com/env/support/LI-7200RS/topics/gas-analyzer-diagnostics.html) and [here](https://www.licor.com/env/support/LI-7200RS/topics/diagnostics-eddypro.html#Gas) and on the 7700 diagnostics [here](https://www.licor.com/env/support/LI-7700/topics/data-files.html).

'Seconds', 'Nanoseconds', 'Sequence Number', 'Diagnostic Value',
'Diagnostic Value 2', 'CO2 Absorptance', 'H2O Absorptance',
'CO2 (mmol/m^3)', 'CO2 (mg/m^3)', 'H2O (mmol/m^3)', 'H2O (g/m^3)',
'Block Temperature (C)', 'Total Pressure (kPa)',
'Box Pressure (kPa)', 'Head Pressure (kPa)', 'Aux 1 - U (m/s)',
'Aux 2 - V (m/s)', 'Aux 3 - W (m/s)', 'Aux 4 - SOS (m/s)',
'Cooler Voltage (V)', 'Chopper Cooler Voltage (V)',
'Vin SmartFlux (V)', 'CO2 (umol/mol)', 'CO2 dry(umol/mol)',
'H2O (mmol/mol)', 'H2O dry(mmol/mol)', 'Dew Point (C)',
'Cell Temperature (C)', 'Temperature In (C)',
'Temperature Out (C)', 'Average Signal Strength',
'CO2 Signal Strength', 'H2O Signal Strength',
'Delta Signal Strength', 'Flow Rate (slpm)', 'Flow Rate (lpm)',
'Flow Pressure (kPa)', 'Flow Power (V)', 'Flow Drive (%)',
'H2O Sample', 'H2O Reference', 'CO2 Sample', 'CO2 Reference',
'HIT Power (W)', 'Vin HIT (V)', 'CH4 Seconds', 'CH4 (umol/mol)',
'CH4 (mmol/m^3)', 'CH4 Temperature', 'CH4 Pressure',
'CH4 Signal Strength', 'CH4 Diagnostic Value', 'CH4 Drop Rate (%)',
'CHK'

## Status Values

More info [here](https://www.licor.com/env/support/LI-7700/topics/data-files.html)

'MSEC', 'SECONDS', 'NANOSECONDS', 'DIAG', 'RSSI', 'REFRSSI',
'LCTSETPT', 'LCTACTUAL', 'BCTSETPT', 'BCTACTUAL', 'CHASSISTEMP',
'OPTICSTEMP', 'OPTICSRH', 'AUXREFTEMP', 'MOTORSETPT',
'MOTORACTUAL', 'USB', 'USBCAPACITY', 'USBFREESPACE', 'REF', 'GND',
'OPTICSTDELTA', 'BOTTOMHEATERW', 'TOPHEATERW', 'CHK'

## co2app.conf

Haven't been able to find any useful metadata about this file.  Its is not in an easy to read format, so I've added "formatted" examples under example files.  It contains matadata for the 7200/7500 calibrations including the timing of the current calibration and coefficient values.  It also includes a lot of data that is repeated in the metadata files.  Its not the full configuration file for the 7550 - but it includes a lot of the info.

The sections we are interested in are: **Calibrate** and **Coef**.  
Below are examples of sections we are interested in in .config file format

### Calibrate

[ZeroCO2]
Val=1.17643
Date=Jul 28 2022 at 10:29:32
[SpanCO2]
Val=1.02727
Target=404.2
Tdensity=16.1496
Date=Jul 28 2022 at 10:36:30
[Span2CO2]
Val=0.0000
Target=
Tdensity=
ic=0.117661
act=0.114538
Date=4Cal
[ZeroH2O]
Val=0.986739
Date=Jul 28 2022 at 10:29:42
[SpanH2O]
Val=1.02615
Target=12
Tdensity=569.034
Date=Feb 7 2020 at 15:00:18
[Span2H2O]
Val=0.0000
Target=
Tdensity=
iw=0.0626465
awt=0.0609231
Date=4Cal
[MaxRef]
CX=38736.8
WX=49803.5
Date=Mar 18 2022 at 15:31:48

### Coef

[Current]
SerialNo=72H-0816
[Band]
A=1.15
[CO2]
A=1.05790E+2
B=1.85532E+4
C=8.28945E+6
D=-1.15799E+9
E=1.00378E+11
XS=0.0028
Z=3.84E-05
SD1=3.230E-2
SD2=-6.330E-2
SD3=2.085E+0
[H2O]
A=5.59192E+3
B=5.95452E+6
C=-5.74709E+8
XS=-0.0012
Z=-1.96E-04
SD1=1.910E-2
SD2=1.323E+0
SD3=2.385E+0
[Pressure]
A0=57.704
A1=15.424
[DPressure]
A0=0.000
A1=1.000
SerialNo=72H-0816
[MaxRef]
B=0.050
C=0.051