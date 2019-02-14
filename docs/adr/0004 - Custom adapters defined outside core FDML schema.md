# 0004 - Custom adapters defined outside core FDML schema

* Status: proposed  
* Date: 2019-02-12


## Context and Problem Statement

There are an unlimited number of adapters that can be defined by the FDML schema syntax.  A way to reduce complexity suggests moving adapter schemas out of the core FDML schema.

## Decision Drivers 

* keep the main schema simple
* increased complexity of multiple schema files
* 
## Considered Options

* one master schema
* core schema which is reference dby specialized adapter schemas

## Decision Outcome

Chosen option: Keep a core FDML.xsd separate from specialized adapter schemas

### Positive Consequences <!-- optional -->

* the core schema will be stable for periods of months
* many adapter schemas can be created without changing the core schema

### Negative Consequences 

* some datasets will require two linked schemas 

