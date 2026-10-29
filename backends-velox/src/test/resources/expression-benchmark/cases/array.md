# array

| case | inputs | expression | description |
| --- | --- | --- | --- |
| pair-int | `v = standard.int()` | `array(v, v)` | Two-element integer array construction. |
| pair-string64 | `v = standard.string(length = 64)` | `array(v, v)` | Two-element 64-byte string array construction. |
| if-mixed | `input = standard.long()` | `IF(input % 3 = 0, CAST(NULL AS ARRAY<BIGINT>), IF(input % 3 = 1, array(), array(input, input + 1)))` | Conditional NULL, empty and two-element array construction. |
| if-struct-mixed | `input = standard.long()` | `IF(input % 3 = 0, CAST(NULL AS ARRAY<STRUCT<v:BIGINT,w:BIGINT>>), IF(input % 3 = 1, array(), array(named_struct('v', input, 'w', input + 1))))` | Conditional NULL, empty and struct array construction. |
| remainder-const3 | `input = standard.long()` | `input % 3` | Integral remainder by small constant divisor. |
