# max

| case | inputs | expression | description | grouping |
| --- | --- | --- | --- | --- |
| global | `v = standard.long()` | `max(v)` | Partial, local shuffle and Final. | - |
| grouped | `k = standard.long(); v = standard.long()` | `max(v)` | Configurable input partitions, local shuffle and Final. | `k` |
