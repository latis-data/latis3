# Design Decisions

Important decisions are made throughout the evolution of a project that sometimes seem arbitrary or simply wrong to new members to the team.  By documenting why decisions were made, a deeper understanding of tradeoffs can give team members an appreciation for why decisions were made.  More importantly, as requirements and circumstances change, previous design decisions may need to be re-visited and modified to meet the needs of a project.  Understanding the full pros and cons of design decisions will enhance the confidence to reverse previous decisions when it becomes necessary to do so.

* *DataType* and *Data* objects are completely decoupled:  
  * Con: Trying to maintain two identical tree structures is difficult.
  * Pro: Computation efficiencies require data to be unencumbered with metadata and model concepts. 
* *Sample* objects are not implemented as a tuple of domain and range, as a *Function* is, but are instead implemented as an integer representing dimensionality followed by the domain and range combined into a single list:  
  * Con: The dimensionality number feels arbitrary. Simply breaking a *Sample* into domain and range would be more elegant.
  * Pro: Computations performed on *Samples* must be efficient.  Extra structure to the data will just slow down the application of functions to the data.
* Sample objects can contain nested *FunctionData* objects, but not nested *TupleData* objects:  
  * Con: Inconsistency between *TupleData* and *FunctionData*.
  * Pro: The model contains all the information needed to represent nested tuples.
