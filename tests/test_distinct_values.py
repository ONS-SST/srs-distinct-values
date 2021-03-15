from unittest.mock import patch, MagicMock

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from distinct_values.distinct_values import compute_distinct_values


@pytest.fixture
def input_csv():
	import os
	import dask.dataframe as dd
	
	csv = pd.DataFrame({
		'id': ['1', '2', '3', '4', '5'],
		'name': ['Steven', 'John', 'Steven', 'Cathy', 'Sonia'],
		'age': ['22', '23', '22', '22', '22']
	})
	csv = dd.from_pandas(csv, npartitions=1)
	
	yield csv
	
	# if input(f"Delete these files? [yes/no] {os.listdir('./tests/test_data')}") == 'yes':
		# print("DELETED")
	# os.rmdir('./tests/test_distinct_values')
	

@pytest.fixture
def expected_data():
	return {
		'id': pd.DataFrame(
				data={'count': [1, 1, 1, 1, 1]},
				index=pd.Index([1, 2, 3, 4, 5], name='id'),
			),
		'name':pd.DataFrame(
				data={'count': [2, 1, 1, 1]},
				index=pd.Index(['Steven', 'John', 'Cathy', 'Sonia'], name='name')
			),
		'age': pd.DataFrame(
				data={'count': [4, 1]},
				index=pd.Index([22, 23], name='age')
			)
	}


def test_compute_distinct_values_within_max_row_limit(input_csv, expected_data):
	with patch('dask.dataframe.read_csv', MagicMock(side_effect=[input_csv])):
		compute_distinct_values(
			filepath='./tests/test_data.csv',
			output_directory='./tests/',
			max_rows_per_file=100
		)
	
	for col in ['id', 'name', 'age']:
		result = pd.read_csv(f'./tests/test_data/{col}.csv', index_col=0)
		assert_frame_equal(expected_data[col].sort_index(), result.sort_index())
		

def test_compute_distinct_values_outside_max_row_limit(input_csv, expected_data):
	with patch('dask.dataframe.read_csv', MagicMock(side_effect=[input_csv])):
		compute_distinct_values(
			filepath='./tests/test_data.csv',
			output_directory='./tests/',
			max_rows_per_file=2
		)
	result_age = pd.read_csv('./tests/test_data/age.csv', index_col=0)
	result_id = pd.read_csv('./tests/test_data/id-part-0.csv', index_col=0)\
					.append(pd.read_csv('./tests/test_data/id-part-1.csv', index_col=0))\
					.append(pd.read_csv('./tests/test_data/id-part-2.csv', index_col=0))
	result_name = pd.read_csv('./tests/test_data/name-part-0.csv', index_col=0)\
					.append(pd.read_csv('./tests/test_data/name-part-1.csv', index_col=0))
					
	assert_frame_equal(expected_data['age'].sort_index(), result_age.sort_index()) 
	assert_frame_equal(expected_data['id'].sort_index(), result_id.sort_index()) 
	assert_frame_equal(expected_data['name'].sort_index(), result_name.sort_index())
