# 0003 - Sample objects implemented as domain-range tuple

* Status: proposed  
* Date: 2019-02-12


## Context and Problem Statement

Sample objects are a fundamental concept in the functional data model.  Several different approaches to represent Sample objects are available.

## Decision Drivers 

* performance
* conceptual integrity
* mathematical correctness

## Considered Options

* samples as a single tuple consisting of an integer describing size of domain followed by domain and range variables 
* samples consisting of a vector of domain objects and a vector of range objects

## Decision Outcome

Domain variables and range variables will be separated in two vectors

### Positive Consequences 

* removing the integer describing the size of the domain reduces complexity
* concept of functions as a domain mapping to a domain is emphasized

### Negative Consequences 

* samples are now broken down into 2 parts

