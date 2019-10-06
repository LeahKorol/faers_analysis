# The Cardiovascular Safety of Anti-Obesity Drugs - Analysis of Signals in the FDA Adverse Event Report System Database

### Authors

Einat Gorelik; Boris Gorelik;  Reem Masarwa; Amichai Perlman; Bruria Hirsh-Raccah; Ilan Matok1

### Repository maintainer
Boris Gorelik boris@gorelik.net https://gorelik.net

## Running the scripts
The configuration files can be found in the config_files.tar.bz2 file. Unpack the files. Each config is an excel file with several spreadsheets. Each spreadsheet is treated separately.

To perform the analysis, you will need a running [Luigi server](https://github.com/spotify/luigi). When the server is up and running, start the processing using `luigi --module pipeline Faers_Pipeline`. The analysis pipeline isn't complicated and is defined in `pipeline.py`.


