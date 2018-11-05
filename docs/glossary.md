# Glossary
Items that are capitalized are Scala class names.

* **arity** - The number of un-flattened *DataTypes* in the domain of a SampledFunction. This is equivalent to the number of arguments to a computational function.
For example, a function with a domain of (x, y) and wavelength with a range of irradiance: "((x, y), wavelength) -> irradiance" has an arity of 2.
* **Dataset** - A *Dataset* is more than just a set of *Data*. A *Dataset* is a cohesive collection of data and metadata. In Summary, a *Dataset* instance consists of three things:
  1. Global *Metadata*
  2. The *functional data model* representation of the dataset in the form of a *DataType* which includes variable metadata.
  3. The data in the form of a *SampledFunction*.
* **DataType** - A recursive algebraic data type consisting of *Scalars*, *Tuples*, and *Functions*.  The *DataType* is purely in the realm of the model.  Since *Tuples* and Functions themselves contain multiple *DataType* objects they can be nested with arbitrary complexity, meaning a *DataType* can be represented as a tree.  
Also, every *DataType* must contain a *Metadata* object.
* **domain** - The independent variables of a function.
* **fdml** - (Functional Data Model Language) An xml file format to describe the contents of a *Dataset*.  
See the FDML Schema section below.
* **Function** - A *Function* is a *DataType* consisting of exactly two *DataType* objects, a domain and a range. Function is a member of the DataType algebraic data type.
* **functional data model** - A relational data model that specifies that relations are functions.  Datsets consist of ordered sequences of samples that are decoupled from metadata.  Operations that implement the functional algebra can be performed on these samples to create new datasets.
* **Metadata** - An instance of *Metadata* is simply a list of key/value pairs to describe a *DataType* or *Dataset*.  Every *Scalar* object is required to contain a *Metadata* object, but *Metadata* objects may be empty for *Tuple* and *Function* objects.
* **range** - The dependent variables of a function.
* **Sample** - A *Sample* is a lean representation of the data values in one sample of a *SampledFunction*. It is a *Tuple2* (pair) of *DomainData* and *RangeData* which are type aliases for *Array[Any]*.  The values can be scalar values or SampledFunctions (in the range).
* **SampledFunction** - The data representation of a Dataset with the *Function* *DataType*. A *SampledFunction* can provide an fs2.Stream of *Samples* and be "evaluated" at a given domain value.
* **Scalar** - A *Scalar* is a *DataType* consisting of a single atomic variable.  A *DataType* of arbitrary complexity must ultimately consist of *Scalar* objects as the leaf nodes of the representation tree.  *Scalar* is a member of the *DataType* algebraic data type.
* **Tuple** - A *Tuple* is a *DataType* consisting of a list of *DataType* objects.  Unlike a function, the *Datatype* objects in a *Tuple* are associated but are not dependent on each other.  
An example is a 2D point consisting of the x,y *Tuple*.  The scalars x and y are associated but x is not a function of y nor is y a function of x. Elements of a *Tuple* also do not have to be of the same type.  Tuple is a member of the *DataType* algebraic data type.
