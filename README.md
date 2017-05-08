# twitter-crime-prediction

This is meant to be run on NYU's HPC cluster `dumbo`, which has Spark and Hadoop pre-installed with the PySpark API working. It may run on other setups, but this is not guaranteed. 

## Requirements

Requirement v<version_number> (module load command on `dumbo`):
- Spark v2.1.0 (`module load spark/2.1.0`)
- Python v2.7.11 (`module load python/gnu/2.7.11`)

## Run Instructions

To run the script, navigate to the top directory of the project in a terminal and run the following command:
```
spark-submit run.py
```
The file will log its output to the file "script_log.txt", which will be written to whatever the current working directory is in the Python script. 
