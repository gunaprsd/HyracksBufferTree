This project is a prototype implementation of an on-disk indexing structure called buffer trees. [Buffer Trees](http://www.cc.gatech.edu/~bader/COURSES/GATECH/CSE-Algs-Fall2013/papers/Arg03.pdf) were introduced by Lars Arge in 2003. They are an extension of the traditional B-Trees which are optimized for writes. Buffer trees are known to provide guaranteed bounds on number of seeks required to access a tuple from disk for a given size of the database.


In addition to the idea of buffer-trees, this implementation includes some optimizations for primary-key indices using bloom filters. 