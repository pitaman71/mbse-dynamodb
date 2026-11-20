"""mbse-schemas stores kept in DynamoDB (mbse-basis [FEA-006: persistence](mbse:mbse-basis/docs/features/FEA-006.md)): `Store`, a
store kept in DynamoDB, the lowest layer of a stack, which interprets each transaction with an interpreter of its own
over its index (`Layer`), its journal the record and no snapshot
([CAP-289: journals kept in DynamoDB](mbse:mbse-basis/docs/capabilities/README.md#CAP-289)), which mbse-rest's service
serves ([CAP-290: a service's store stacked, with no snapshot](mbse:mbse-basis/docs/capabilities/README.md#CAP-290))."""
