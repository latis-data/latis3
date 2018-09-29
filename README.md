# Summary

*LaTiS version 3* is a rewrite of the *Functional Data Model* and *Algebra* based on lessons learned from version 2, and with a bit more inspiration from Functional Programming. It will incubate as *LaTiS3 version 0.x* until we are ready to finalize the API and release it as *LaTiS version 3.x*.

The primary distinction of LaTiS 3 is the perspective of a *Dataset* as a sequence of *Samples* with *Operations* that implement the *Functional Algebra* in terms of pure functions applied to *Samples*. Special implementations of *FunctionData* facilitate *nested Functions* and optimal operations on data.

Another distinction of LaTiS 3 is to completely de-couple the model from the data.  The *Data* algebraic data type has no knowledge of the *DataType* algebraic data type and vice versa.  Only at the *Dataset* level do the two concepts become combined.

The usual collection of *Adapters* and *Writers* are conceptually similar but take better advantage of the new data model implementation. The *Adapter* life cycle, in particular, is much cleaner. This new version of LaTiS will continue to support the same service interface (and more).

see glossary [here](docs/glossary.md)

see design decisions [here](docs/design_decisions.md)
