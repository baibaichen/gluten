# approx_count_distinct

| case | inputs | expression | description | grouping |
| --- | --- | --- | --- | --- |
| long-g1024 | `id = standard.long()` | `approx_count_distinct(id, 0.05)` | HLL with relative SD 0.05 and 1024 groups. | `pmod(id, 1024)` |
| projected-count | `id = standard.long()` | `approx_count_distinct(id, 0.05) + 1000000` | HLL result with an additive post-projection. | - |
| string-g1024 | `id = standard.long(); value = standard.string()` | `approx_count_distinct(value, 0.05)` | String HLL with relative SD 0.05 and 1024 groups. | `pmod(id, 1024)` |
| repeated-long | `id = standard.long()` | `approx_count_distinct(pmod(id, 2), 0.05)` | Two distinct values repeated across map partitions. | - |
| nan-payloads | `v = nanPayloadDouble()` | `approx_count_distinct(v, 0.05)` | Known issue: distinct NaN payloads must count as one value. | - |
