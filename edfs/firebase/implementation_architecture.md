# Firebase Implementation Notes
---
## Data Partitioning

Data is partitioned by lines. If a file is added to k partitions, line(i) is placed in partition p such that (i+p)%k = 0. Partition location for a file are stored in the namenode object, in the format "{DATANODE}-{KEY}". Lines in a partition are seperated by the "\\\\n" character because Firebase does not support "\n" in it's inputs.

---
## Example Firebase Database Contents

```
{
  "namenode": {
    "root": {
      "user": {
        "john": {
          "cars-json": {
            "p1": "datanode_1-1",
            "p2": "datanode_2-1"
          }
        },
        "mary": 0
      }
    }
  },
  "datanode_1": {
    "1": "cars.json line 1"
  },
  "datanode_2": {
    "1": "cars.json line 2"
  }
}
```
`NOTE: "cars.json" must be stored with the key "cars-json" because Firebase does not allos periods in keys`

`NOTE 2: Empty directories have the data "0". This is because Firebase does not support empty json objects so a stand-in must be used.`