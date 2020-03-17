 # dbtree
 [![Maven Central](https://img.shields.io/maven-central/v/com.github.kdbtree/kdbtree.svg?label=Maven%20Central&color=success&style=plastic)](https://search.maven.org/search?q=g:%22com.github.kdbtree%22%20AND%20a:%22kdbtree%22)
![GitHub](https://img.shields.io/github/license/kdbtree/kdbtree?color=informational&style=plastic)

The `dbtree` library is a realization of the DB-tree 
data structure 
described in [[1]](#1) (see bibliography), intended to be persistent
in a database (Postgres and MySQL natively supported,
 at the moment).

DB-trees enable to realize persistent _authenticated data structures (ADSes)_, see [[3]](#3). ADSes are usually presented as in-memory data structures and it is not immediate to realize them in a database, efficiently.  

DB-trees also enable to realize indexes to speed up _aggregate range queries_
corresponding for example to the following SQL. 
```sql
SELECT SUM(value)
FROM sales
WHERE '2020-01-01'<=sales.date AND sales.date<='2020-03-03'
```
These kind of queries are hardly optimized by DBMSes.
A support for _aggregate group-by range queries_ is also
 available. 
 
Essentially, DB-trees are a kind of _overlay-indexes_,
 that is a way to represent an index in a database to support
 use cases that are not supported by the DBMS indexes natively. 
For the effectiveness of DB-trees and a comparison with other techniques see the scientific paper [[1]](#1).

<!-- 
### Overlay Indexes
That of the Overlay Indexes is a novel family of structures which 
are meant to be stored in standard databases in order to complement 
the capabilities they were designed for.
Overlay Indexes turn out being of great help for those applications 
that:

 •  Need to provide support for specific **operations** (over the 
    persisted data-set) the DBMS doesn't support instead.
    
 •  Such operations have **strong efficiency requirements** (see, 
    for example, interactive applications).
    
 •  The **large amount of data** prohibits the adoption of in-memory 
    solutions, hence such efficiency requirements can only be 
    fulfilled employing some form of indexing.
 

### DB-tree
The **DB-tree** is an Overlay Index facilitating two main services:

1.  Efficiently performing **[Group-By] Aggregate Range Queries**;
2.  Realizing a persistent **Authenticated Data Structure (ADS);**

In the following, we offer a brief insight of the DB-tree data 
structure, preparatory for the comprehension of this library. 

For an integral documentation about the DB-tree and Overlay 
Indexes in general, we recommend reading the 
<a href="https://ieeexplore.ieee.org/document/8919979">official paper</a>.

### The Data Structure
<img src="https://ieeexplore.ieee.org/mediastore_new/IEEE/content/media/6287639/8600701/8919979/pizzo3-2957346-large.gif" alt="DB-tree image" width="600" height="300">
<br><br>
The DB-tree is a randomized and hierarchical data structure, orderly 
storing key-value pairs in a DBMS. The DB-tree was inspired, in its 
design, by two other well known data structures: B-tree and Skip List.

1.  **B-tree**: the DB-tree structure strongly resembles that of a 
    B-tree, with the root node, at the highest level, storing pairs 
    and pointers directed to nodes at lower ones. At every moment, 
    the total order with respect to the keys is preserved within and 
    among nodes. Being, as a matter of fact, the usage of pointers in 
    DBMS quite unhandy, in DB-trees they were replaced by aggregate 
    values. These do not directly point to their corresponding child 
    nodes, yet resume their information content. Hence, referencing by 
    pointers is substituted by referencing via level and key containment.
2.  **Skip List**: the pairs within each node are not assigned 
    deterministically. In B-trees, the descent to a lower level is 
    led by the exigence of rebalancing the number of elements stored 
    per node. In DB-trees, on the contrary, levels are 
    probabilistically assigned to elements according to the same level 
    extraction algorithm employed for Skip Lists' plateau nodes.

Therefore, we can imagine a DB-tree as a Skip List whose _tower nodes_ 
were totally discarded, and whose _plateau nodes_ (namely their key-value 
pairs) were organized in a B-tree fashion. Furthermore, the space 
reserved for each B-tree pointer was recycled to host the aggregate 
value of the pointed child, resulting by the application of some 
decomposable aggregation function to all its stored values.

Finally, we define the **decomposable aggregation function** _α_ as a 
composition of three functions α ≔ 〈f, g, h〉 such that 
_α(v₁, ... , vₙ) = h( f( g(v₁), ... , g(vₙ) ) )_, where:

1.  g: V -> A, is the component transforming each atomic value 
    into the corresponding interpretation in the aggregate domain.
2.  f: A1 x ... x An -> A, is the **core aggregation function**, 
    deriving a single output aggregate value out of
    multiple input ones.
3.  h: A -> D, is the component which transforms an aggregate 
    value into the corresponding interpretation in the expected 
    α output domain.

### [Group-By] Aggregate Range Query
The first benefit offered by a DB-tree is the ability to perform 
**aggregate range queries** fast and efficiently. An aggregate
range query deals with 
performing the aggregation of those values associated with keys 
spanning within some given range. 

Thanks to the high number 
of retained aggregate values and to its probabilistic 
properties, a DB-tree can serve aggregate range queries: 
(1) retrieving all necessary nodes in just **one query round**; 
(2) employing both **logarithmic** spatial occupancy and time 
complexity with respect to the amount of persisted data.

In addition, when it's built on top of composite key types 
(divisible into most and least significant partitions), 
a DB-tree can serve Group-By aggregate range query, i.e. 
the aggregation of values associated with keys whose least 
significant part is contained within some given range, 
performed on the basis of distinct values of the most 
significant part (_namely group_).

### Authenticated DB-tree
When α identifies with an hashing function, the DB-tree actually represents 
a **persistent authenticated data structure (ADS)** 
(_lit. authenticated DB-tree_).

The authenticated DB-tree retains as root aggregate value, 
the root hash of the entire data-set. Moreover, it exposes 
the **authenticated query** primitive, which returns the queried 
value together with the corresponding proof of validity, 
i.e. the exact sequence of steps for computing (starting 
from the queried value) the root-hash, according to the 
hashing scheme it was derived from.

Since there's a one-to-one relationship between the root-hash 
and the content of an ADS, we have to make sure to **de-randomize** 
somehow the level extraction algorithm, yet preserving its 
probabilistic properties. As for authenticated Skip Lists, 
this can be achieved using each new key as seed for the 
level's random extraction for the corresponding pair.

Again, due to its probabilistic properties and to the 
referencing via level and key containment mechanism, an 
authenticated DB-tree can serve authenticated queries: 
(1) retrieving all necessary nodes in just **one query round**; 
(2) employing both **logarithmic** spatial occupancy and time 
complexity with respect to the amount of persisted data.
-->
# The Library
This library represents the implementation of the concepts 
seen so far, offering to the user the possibility to:
1.  **Insert/Update/Delete a key-value pair from a DB-tree.**
2.  **Perform [Group-By] aggregate range queries.**
3.  **Instantiate and use an authenticated DB-tree.**

Moreover, this library is completely extensible on user-side, since 
it allows to:
1.  **Define custom decomposable aggregate functions.**
2.  **Define custom [composite] key types.**
3.  **Defining custom DBMS connectors**, without constraining 
    the application logic to the specific underlying technology.

Further information about these modules can be directly encountered 
in the related interfaces' documentation: `Function`, `KeyParser` and `Connector`.

Moreover, we offer:
<ol>
    <li>An example package, providing some already-implemented modules.</li>
    <li>An example script, written in Kotlin, showing how this 
    library should be used.</li>
</ol>

# Reference Guide
In the following, we present a brief tutorial about how to use this library 
to (1) realize custom DB-tree modules for your specific purposes, 
(2) using the resulting DB-tree.

 1. Download and include the .jar archive (or the source code directly) 
    into your own project, then import the package `it.uniroma3.dbtree.model.*`. 
    
    In the next points, the essential (implementation specific) 
    components of a DB-tree are illustrated. You can find some 
    already implemented examples in the package 
    `it.uniroma3.dbtree.examples.*` to get started.
 2. Implement the **custom aggregation function** you need your DB-tree 
    to maintain:
    `val aggrFun = MyFunction()`, being the class `MyFunction` an 
    extension of the library's interface `Function<V,A,D>`. 
    This object will be the one in charge of actually 
    managing the aggregation among different values.
 3. Implement your **key parser**: `val kp = MyKeyParser()`, being 
    `MyKeyParser` an extension of the library's interface `KeyParser<K>`. 
    This object will be in charge of managing all meta-operations 
    related with keys, e.g. key parsing or comparisons' translation 
    for the interpretation by the underlying DBMS 
    in case of composite (non-primitive) key types. Further details can be 
    seen directly in the documentation.
 4. Implement your particular **database connector**: 
    `val c = MyConnector(url,table,kp)`, being `MyConnector` an 
    extension of the library's interface `Connector<K>`. This 
    object will be the one in charge of establishing and 
    exploiting the connection with the underlying DBMS, 
    managing the retrieval and the modification of 
    the persisted DB-tree nodes.
 5. Finally, the **DB-tree** class can be instantiated: 
    `val dbtree = DBTree(aggrF,c)`, and used every time 
    (1) an operation on the persisted data-set is invoked 
    (to keep the DB-tree synchronized with the
    associated data-set), 
    (b) some DB-tree procedure is needed, 
    e.g. `dbtree.rangeQuery(k',k")`.





# Bibliography

<a name="1">[1]</a>  Diego Pennino, Maurizio Pizzonia, Alessio Papi. [Overlay Indexes: Efficiently Supporting Aggregate Range Queries and Authenticated Data Structures in Off-the-Shelf Databases.](https://ieeexplore.ieee.org/abstract/document/8919979) IEEE Access. 7:175642-175670. 2019.

<a name="2">[2]</a> [Wikipidia page about Merkle tree.](https://en.wikipedia.org/wiki/Merkle_tree)

<a name="3">[3]</a> Tamassia, Roberto. [Authenticated data structures.](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.93.9836&rep=rep1&type=pdf) European symposium on algorithms. Springer, Berlin, Heidelberg, 2003.

<a name="4">[4]</a> https://en.wikipedia.org/wiki/Aggregate_function


