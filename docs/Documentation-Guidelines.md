#LaTiS Guidelines for Effective Documentation

###Architectural Decision Records:

Documenting important design decisions is important to guide future development.  Once decisions have been made and documented, new and existing team members can be guided to do the right things as they are writing code.  Operating from a consistent set of principles frees up developers to think about the task at hand, instead of constantly revisiting the same architectural issues.

After reviewing the architectural decision records, new team members can quickly get up to speed and begin operating with the same assumptions as the rest of the team.  It is always good to encourage new ideas, but randomly heading in different directions does not help the design coherence of a complex software product.

Keeping decision records up to date is even harder to do than updating documentation embedded in source code since they are not embedded with the source code as Scaladocs are.  Out of date documentation is probably more harmful than no documentation so it is important to not let technical debt accumulate in the decision records.  As important decisions are made, they must be documented while the ideas are fresh in everyone’s minds.  Once a few weeks pass, a lot of the subleties of why a decision has been made will be forgotten.

###Scaladoc for Classes:

The comments in the Scaladocs should be considered an important and essential part of the source code.  If textual documentation is just considered to be “code” at a higher level of abstraction than executable source code, then the value of Scaladocs becomes obvious.  An entire application should be comprehended simply by reading the Scaladocs without having to dive into the code itself.

The biggest challenge of writing documentation is to keep it current.  As mentioned in the above section, out of date docs are extremely harmful.  Unfortunately, there are no unit tests for documentation.  So as code is refactored, and tests updated, the docs can quickly become out of date.

Everything is fair game to be included in Scaladocs.  The DRY principle of course should be followed which encourages Scaladoc authors to provide links to other classes or architectural decision records where appropriate.  One of the most useful items to include is an actual code sample that shows how the code is actually used.  This is rarely seen in Scaladocs, but is a direct bridge to the source code itself.  Explaining how a class fits in with other classes can make a complex application comprehensible.  Taking the writing of documentation seriously provides large dividends in the future.