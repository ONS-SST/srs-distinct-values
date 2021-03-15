from distinct_values.distinct_values import compute_distinct_values
from distinct_values.parse_settings import parse_settings


def main():
	settings = parse_settings()
	compute_distinct_values(
		filepath=settings['full-input-file-path'],
		output_directory=settings['full-output-directory'],
		max_rows_per_file=settings['max-row-limit']
	)
