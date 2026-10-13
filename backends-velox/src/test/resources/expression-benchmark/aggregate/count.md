# count

| case | inputs | expression | description | grouping |
| --- | --- | --- | --- | --- |
| global | `v = standard.long()` | `count(v)` | Partial, local shuffle and Final. | - |
| grouped | `k = standard.long(); v = standard.long()` | `count(v)` | Configurable input partitions, local shuffle and Final. | `k` |
