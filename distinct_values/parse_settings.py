import os
import yaml


class SettingsError(Exception):
	pass
	

def parse_settings() -> dict:
	with open('./settings.yaml') as yaml_file:
		settings = yaml.load(yaml_file, Loader=yaml.FullLoader)
	
	validate_give_keys(settings, expected_keys={'full-input-file-path', 'full-output-directory', 'max-row-limit'})
	
	validate_input_file_path(settings['full-input-file-path'])
	
	validate_output_directory(settings['full-output-directory'])
	
	validate_max_row_limit(settings['max-row-limit'])
	
	return settings
	

def validate_give_keys(settings: dict, expected_keys: {str}) -> None:
	missing_keys = expected_keys.difference(settings.keys())
	if missing_keys != set():
		raise SettingsError('Please add the following keys back to settings.yaml:\n'
							f'{missing_keys}\n'
							'See ./README.md for an example.')


def validate_input_file_path(filepath) -> None:
	if not isinstance(filepath, str):
		raise SettingsError(f'"full-input-file-path" must be a string, not: {filepath}.')
	
	if not os.path.isfile(filepath):
		raise SettingsError(f'"full-input-file-path" {filepath} does not exist.')
		
	if not filepath.endswith('.csv'):
		raise SettingsError('"full-input-file-path" must be a csv file.')
		

def validate_output_directory(directory) -> None:
	if not isinstance(directory, str):
		raise SettingsError(f'"full-output-directory" must be a string, not: {directory}.')
	

def validate_max_row_limit(limit) -> None:
	if not isinstance(limit, int):
		raise SettingsError(f'"full-output-directory" must be a integer, not: {limit}.')
