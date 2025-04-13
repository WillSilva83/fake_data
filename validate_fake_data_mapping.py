from libs import fake_data_with_mapping  as fake_wm
from libs import fake_data as fd

file_mapping = "mapping_table_test.json"

table_name = "table1_test"
database_name = "database_test"

json_mapping = fake_wm.read_mapping_json(file_mapping)


table_metadata = fd.get_table(database_name, table_name)


print(table_metadata)