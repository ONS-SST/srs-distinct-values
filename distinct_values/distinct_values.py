import os

import dask.dataframe as dd
from tqdm import tqdm


def compute_distinct_values(filepath: str, output_directory: str, max_rows_per_file: int) -> None:
	input_directory, file_name = os.path.split(filepath)
	file_name = file_name.rsplit('.', maxsplit=1)[0]
	
	output_directory = os.path.join(output_directory, file_name)
	
	data = dd.read_csv(filepath, dtype='object')
	
	os.makedirs(output_directory, exist_ok=True)
	
	for column in tqdm(data.columns, desc=f'{file_name} columns:'):
		column_value_counts = data[column].value_counts()
		number_of_rows_to_write = data[column].nunique().compute()
		
		n_partitions, single_file, file_suffix = write_to_single_file_check(number_of_rows_to_write, max_rows_per_file)
		
		if column_value_counts.npartitions != n_partitions:
			column_value_counts.repartition(npartitions=n_partitions)
		
		column_value_counts.to_csv(
			filename=os.path.join(output_directory, column + file_suffix),
			single_file=single_file,
			header=['count'],
			index_label=column
		)
		
	print(f'\nJob has finished. Results have been written out to: {output_directory}\n')


def write_to_single_file_check(given_rows: int, limit: int) -> (int, bool, str):
	if given_rows > limit:
		n_partitions, remainder = divmod(given_rows, limit)
		n_partitions += bool(remainder)
		single_file = False
		file_suffix = '-*.csv'
	
	else:
		n_partitions = 1
		single_file = True
		file_suffix = '.csv'
	
	return n_partitions, single_file, file_suffix
