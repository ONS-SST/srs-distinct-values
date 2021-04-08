# Compute Distinct Values

This tool will compute and write out all distinct values in all columns in any given CSV file.

# Installation

## Using venv with requirements.txt

First set-up your python virtual environment by opening the command prompt to the `srs-distinct-values` folder. 

In the command prompt execute:

```
python -m venv ./dvt-env
```
This will make a new directory in the folder called `./dvt-env`.

Then activate the virtual environment by executing:
```
dvt-env\Scripts\activate.bat
```
If successful, your command prompt will be prefixed with `(dvt-env)` to signify you're operating within the python virtual environment. 

Now install the required libraries using:
```
pip install -r requirements.txt
```

The tool is ready to use once all the requirements are successfully installed.

**Note:**
If you're installing libraries from artifactory you must set-up your credentials first to point pip to artifactory instead of pypi. 

# Usage

## `settings.yaml`

The `./settings.yaml` allows the user to control:

* full-input-file-path: Full file path to the CSV file.
* full-output-directory: The directory which the tool will write the distinct values out too.
* max-row-limit: The maximum number of rows per output file. At the time of development, the excel row limit is 1048576.

Appropriate values must be assigned to all of the above in the `./settings.yaml` for the tool to run.

### Example:

```yaml
full-input-file-path: D:\projects\dummy\data\Primary_Pupils_Dummy_Dataset.csv

full-output-directory: D:\projects\srs-distinct-values

max-row-limit: 500
```

**Note:** 
For the `./settings.yaml` to be parsed correctly, the structure of the file must be:
`KEY: VALUE`.

## Executing the Tool

Once the `./settings.yaml` has been completed with the appropriate desired values, double click the `./Run-Distinct-Values-Tool.bat`. This will launch a command prompt and the tool will begin computing the distinct values. A loading bar will appear in the command prompt displaying the progress the tool has made through the columns found in the CSV file. 

**Note:** 
If the tool fails to parse the settings given in `./settings.yaml` you will find a `SettingsError` in the command prompt which will help you troubleshoot the cause of the error.
