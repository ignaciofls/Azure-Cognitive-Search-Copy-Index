# Azure-Cognitive-Search-Copy-Index

Sample python code to copy Azure Cognitive Search indexes

## How to run it
  python main.py --parameter

| Parameter                          | Value                                                      | Note                                           |
|---------------------------------- |------------------------------------------------------------|------------------------------------------------|
| --src_service          			| SOURCE SERVICE              								 | Required									      |
| --src_service_key      			| SOURCE SERVICE KEY        								 | Required                                       |
| --src_index            			| SOURCE INDEX 											     | Required                                       |
| --dst_service          			| DESTINATION SERVICE         								 | Leave empty if you want to copy to src_service |
| --dst_service_key      			| DESTINATION SERVICE KEY   								 | Leave empty if you want to copy to src_service |
| --dst_index            			| DESTINATION INDEX 										 | Required                                       |
| --filter_by            			| String filterable field used to batch read/write operation | REQUIRED                                       |
