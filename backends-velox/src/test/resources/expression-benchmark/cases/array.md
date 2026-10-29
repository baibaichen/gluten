# array

| case | inputs | expression | description |
| --- | --- | --- | --- |
| pair-int-repeated | `v = standard.int()` | `array(v, v)` | Two-element integer array construction with repeated input reference. |
| pair-long-distinct | `input = standard.long()` | `array(input, input + 1)` | Two-element long array construction with distinct values. |
| pair-string64-repeated | `v = standard.string(length = 64)` | `array(v, v)` | Two-element 64-byte string array construction with repeated input reference. |
| pair-string64-distinct | `k = standard.string(length = 64); v = standard.text()` | `array(k, v)` | Two-element string array construction with distinct inputs. |
| single-struct | `input = standard.long()` | `array(named_struct('v', input, 'w', input + 1))` | Single-element struct array construction. |
