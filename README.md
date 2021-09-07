# Azure-Cognitive-Search-Copy-Index

Sample python code to export, import or duplicate Azure Cognitive Search indices. 

## How to run it
  python main.py --parameter1 value1 --parameter2 value2 ...

| Parameter                          | Value                                                      | Note                                           |
|---------------------------------- |------------------------------------------------------------|------------------------------------------------|
| --src_service          			| SOURCE SERVICE              								 | Required									      |
| --src_service_key      			| SOURCE SERVICE KEY        								 | Required                                       |
| --src_index            			| SOURCE INDEX 											     | Required                                       |
| --dst_service          			| DESTINATION SERVICE         								 | Leave empty if you want to export to file or duplicate to same src_service |
| --dst_service_key      			| DESTINATION SERVICE KEY   								 | Leave empty if you want to export to file or duplicate to same src_service |
| --dst_index            			| DESTINATION INDEX 										 | Required                                       |
| --filter_by            			| String filterable field used to batch read/write operation | Required                                       |
| --action           			| Action to perform, either export or import a backup or just duplicate an index into a new one. Enter export, import or duplicate | Required                                       |

## Examples

To export the aa index into a set of json files to your current folder run this: 
  
 ``` python main.py --src_service xyz --src_service_key xxx --src_index aa --dst_index bb --filter_by language --action export ```
  
This command  will browse your current folder for exported json files to import them into a fresh bb index (that will copy index schema from aa index):
  
```  python main.py --src_service xyz --src_service_key xxx --src_index aa --dst_index bb --filter_by language --action import ```
  
To duplicate an aa index into a new bb index inheriting its schema and populating it with its content use: 
  
```  python main.py --src_service xyz --src_service_key xxx --src_index aa --dst_index bb --filter_by language --action duplicate ```

## Disclaimer

This tool is not production ready and should be deeply tested and customized before using it with live data
