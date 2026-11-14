"""mbse-schemas stores kept in DynamoDB (mbse-basis [FEA-006: persistence](mbse:mbse-basis/docs/features/FEA-006.md)): `Store`, a
store kept in DynamoDB, below a lazy `Builders.Store`, its journal the record and no snapshot
([CAP-289: journals kept in DynamoDB](mbse:mbse-basis/docs/capabilities/README.md#CAP-289)); and `Plugin`, mbse-rest's
persistence plugin, which it supersedes, kept while mbse-rest's service persists through it
([CAP-290: a service's store stacked, with no snapshot](mbse:mbse-basis/docs/capabilities/README.md#CAP-290))."""
