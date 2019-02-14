# 0005 - Operations defined in FDML schema as elements

* Status: proposed  
* Date: 2019-02-12


## Context and Problem Statement

Operations can be defined in the FDML as elements and attributes or simply as elements.  For example the operation take can be described as:

```
<xs:element name="take" type="xs:integer">
```

or with attributes as:

```
<xs:element name="take">
    <xs:complexType>
        <xs:attribute name="value"/>
    </xs:complexType>
</xs:element>
```

## Decision Drivers 

* consistency
* expressiveness

## Considered Options

* operations described as elements only
* operations described as mixture of elements and attributes

## Decision Outcome

Chosen option: Try to describe operations as elements only

### Positive Consequences 

* consistency

### Negative Consequences 

* future operations may not be definable

