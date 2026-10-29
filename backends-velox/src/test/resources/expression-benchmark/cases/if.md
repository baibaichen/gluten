# if

| case | inputs | expression | description |
| --- | --- | --- | --- |
| null-else-array | `input = standard.long(); a = standard.intArray()` | `IF(input % 3 = 0, CAST(NULL AS ARRAY<INT>), a)` | Single IF merging constant NULL and a stored integer array. |
| empty-else-array | `input = standard.long(); a = standard.intArray()` | `IF(input % 3 = 1, CAST(array() AS ARRAY<INT>), a)` | Single IF merging a constant empty array and a stored integer array. |
| nested-stored-array | `input = standard.long(); a = standard.intArray()` | `IF(input % 3 = 0, CAST(NULL AS ARRAY<INT>), IF(input % 3 = 1, CAST(array() AS ARRAY<INT>), a))` | Nested IF merging NULL, empty array and a stored integer array. |
| nested-stored-string64-array | `input = standard.long(); a = standard.stringArray(length = 64)` | `IF(input % 3 = 0, CAST(NULL AS ARRAY<STRING>), IF(input % 3 = 1, CAST(array() AS ARRAY<STRING>), a))` | Nested IF merging NULL, empty array and a stored 64-byte string array. |
| nested-constructed-array | `input = standard.long()` | `IF(input % 3 = 0, CAST(NULL AS ARRAY<BIGINT>), IF(input % 3 = 1, array(), array(input, input + 1)))` | Nested IF merging NULL, empty array and a newly constructed array. |
