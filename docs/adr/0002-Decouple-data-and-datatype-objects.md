# 0002 - Decouple data and datatype objects

* Status: proposed  
* Date: 2019-02-12


## Context and Problem Statement

Previously LaTiS V2 combined Data and Metadata in the Variable trait.  What appeared to be a good object-oriented design decision turns out to create more problems than are solved by combining these two concepts.

## Decision Drivers 

* performance, computing  with metadata is simply inefficient
* separation of concerns
* inconsistency between data and metadata

## Considered Options

* keeping metadata and data together
* separating metadata and data

## Decision Outcome

Chosen option: Data and metadata will be separated by removing metadata from the Sample class.

### Positive Consequences 

* Loading data into Spark will be greatly simplified by removing the metadata
* Applying transformations to the data and the model separately will reduce code complexity

### Negative Consequences 

* data and metadata may become out of sync



