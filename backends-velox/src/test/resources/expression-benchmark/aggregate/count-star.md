# count-star

| case | inputs | expression | description | grouping |
| --- | --- | --- | --- | --- |
| global | `v = standard.long()` | `count(*)` | Partial, local shuffle and Final. | - |
| grouped | `k = standard.long(); v = standard.long()` | `count(*)` | Configurable input partitions, local shuffle and Final. | `k` |
