# rquery

An educational query engine project based on:

* https://github.com/andygrove/how-query-engines-work

## To-Do List

- [ ] next steps: finish and test query planner (if Any casting in Logical Plans really work, otherwise I need to use
  enums here as well),
  dann expressions checken und evtl ueberarbeiten
- [ ] write tests for physical plans
- [ ] write test data (RecordBatches etc) generator or use a crate like fake or quickcheck for that
- [ ] write query planner to check if physical plan works like intended
- [ ] think about how to implement Expressions hierarchy
- [ ] use the same pattern for logical expressions or physical expressions. so either dyn or enum, but dyn must first be
  tested if it really works as intended (see handling of logical plan in query planner)
