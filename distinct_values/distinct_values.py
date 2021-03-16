import os

import pandas as pd
from tqdm import tqdm


def compute_distinct_values(filepath: str, output_directory: str, max_rows_per_file: int) -> None:
	input_directory, file_name = os.path.split(filepath)
	file_name = file_name.rsplit('.', maxsplit=1)[0]
	
	output_directory = os.path.join(output_directory, file_name)
	
	data = pd.read_csv(filepath, dtype='object')
	
	os.makedirs(output_directory, exist_ok=True)
	
	for column in tqdm(data.columns, desc=f'{file_name} columns:'):
		column_value_counts = data[column].value_counts()
		number_of_rows_to_write = data[column].nunique()

		n_partitions = get_number_of_partitions(
			rows_given=number_of_rows_to_write,
			row_limit=max_rows_per_file
		)

		if n_partitions > 1:
			for i in range(n_partitions):
				start = max_rows_per_file * i
				end = min(number_of_rows_to_write, max_rows_per_file * (i + 1))

				column_value_counts.iloc[range(start, end)].to_csv(
					os.path.join(output_directory, f'{column}-part-{i}.csv'),
					header=['count'],
					index_label=column
				)
		else:
			
			column_value_counts.to_csv(
				os.path.join(output_directory, f'{column}.csv'),
				header=['count'],
				index_label=column
			)
		
	print(f'\nJob has finished. Results have been written out to: {output_directory}\n')


def get_number_of_partitions(rows_given: int, row_limit: int):
	if rows_given > row_limit:
		n_partitions, remainder = divmod(rows_given, row_limit)
		n_partitions += bool(remainder)

	else:
		n_partitions = 1

	return n_partitions
