# case_when

| case | inputs | expression | description |
| --- | --- | --- | --- |
| stored-int-array | `input = standard.long(); a = standard.intArray()` | `CASE WHEN input % 3 = 0 THEN CAST(NULL AS ARRAY<INT>) WHEN input % 3 = 1 THEN CAST(array() AS ARRAY<INT>) ELSE a END` | Multi-branch CASE WHEN merging NULL, empty array and a stored integer array. |
| stored-string64-array | `input = standard.long(); a = standard.stringArray(length = 64)` | `CASE WHEN input % 3 = 0 THEN CAST(NULL AS ARRAY<STRING>) WHEN input % 3 = 1 THEN CAST(array() AS ARRAY<STRING>) ELSE a END` | Multi-branch CASE WHEN merging NULL, empty array and a stored 64-byte string array. |
| stored-struct-array | `input = standard.long(); a = standard.mapEntries()` | `CASE WHEN input % 3 = 0 THEN CAST(NULL AS ARRAY<STRUCT<key:STRING,value:INT>>) WHEN input % 3 = 1 THEN CAST(array() AS ARRAY<STRUCT<key:STRING,value:INT>>) ELSE a END` | Multi-branch CASE WHEN merging NULL, empty array and a stored struct array. |
