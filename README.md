# Compute Distinct Values

This tool will compute and write out all distince values in all columns in any given csv file.

# Install

## Using Poetry

## Using venv with requirements.txt

# Instructions

## `settings.yaml`

The `./settings.yaml` allows the user to control:

* full-input-file-path: Full file path to the csv file.
* full-output-directory: The directory which the tool will write the distinct values out too.
* max-row-limit: The maximum number of rows per output file. At the time of development the excel row limit is 1048576.

Appropriate values must be assigned to all of the above in the `./settings.yaml` for the tool to run.

### Example:

```
full-input-file-path: D:\projects\dummy\data\Primary_Pupils_Dummy_Dataset.csv

full-output-directory: D:\projects\srs-distinct-values

max-row-limit: 500
```

**Note**
For the `./settings.yaml` to be parsed correctly, the structure of the file must be:
`KEY: VALUE`.

## Executing the Tool

Once the 

