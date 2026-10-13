# bitmap_construct_agg

| case | inputs | expression | description | grouping |
| --- | --- | --- | --- | --- |
| long-g256 | `id = standard.long()` | `bitmap_construct_agg(pmod(id, 32768))` | Bitmap construction with 256 groups and independent value cardinality. | `pmod(id, 256)` |
