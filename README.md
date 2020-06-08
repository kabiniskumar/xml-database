# XML Database

The repository contains code implemented as part of course CSE 510- Database Management System Implementation.

The project work is divided into three phases. 

### Phase 1
The goal of the first phase is to familiarize ourselves with the components of Minibase, a relational database system developed for educational purposes. 

### Phase 2
The second phase focuses on modifying the underlying modules in Minibase to support the handling of XML data. This is achieved by introducing modifications(new datatype IntervalType) to store XML data and developing query plans to retrieve this data. 

Tasks 
- Parse an XML input file to generate interval labels for each node, creating an XML record and finally inserting into the database.
- Parse a given XML query or pattern tree(based on Ancestor-Descendant and Parent-Child rules) to construct three different queryplans(based on Nested Loop join iterator and Sort Merge join iterator).


### Phase 3
The third phase focuses on further modifying the modules to support complex physical operations on XML data. This includes operations such as Sorting, Group By, Cartesian Product, Joins based on tag and interval values. Furthermore, this phase also focuses on techniques to improve query performance by introducing index structures that work with XML data.

Tasks
- Implement index structures on XML data - Tag-based index and Interval-based index.
- Implement complex physical operators - Cartesian product, Tag join, Interval join, Sort and Group By.
- Implement three efficient query plans to process a complex pattern tree.
