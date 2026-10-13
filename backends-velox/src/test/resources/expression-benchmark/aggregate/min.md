# min

| case | inputs | expression | description | grouping |
| --- | --- | --- | --- | --- |
| global | `v = standard.long()` | `min(v)` | Partial, local shuffle and Final. | - |
| grouped | `k = standard.long(); v = standard.long()` | `min(v)` | Configurable input partitions, local shuffle and Final. | `k` |
