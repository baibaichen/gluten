# avg

| case | inputs | expression | description | grouping |
| --- | --- | --- | --- | --- |
| global | `v = standard.long()` | `avg(v)` | Partial, local shuffle and Final. | - |
| grouped | `k = standard.long(); v = standard.long()` | `avg(v)` | Configurable input partitions, local shuffle and Final. | `k` |
| float-rounding | `v = standard.long()` | `avg(0.1D + pmod(v, 7) * 0.01D)` | Floating accumulation rounding. | - |
