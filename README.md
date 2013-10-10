CustomSamplers
==============

The CustomSamplers project is a fairly big extension to the JMeter tool that 
contains custom configuration and sampler elements for different relational and
non-relation databases, in order to measure performance characteristics of binary
data handling for given workloads.

Currently the following databases are supported by configuration and sampler elements:
NoSQL:
------
 + Cassandra
 + RIAK
 + MongoDB
 + CouchDB
 + Voldemort
 - Accumulo (Pending)
 - HBase (Pending)
 
NewSQL:
-------
 + MariaDB
 + Drizzle
 - TokuDB (Pending)

Relational:
-----------
 + PostgreSQL
 + MySQL
 + Oracle 11g


Known issues:
-------------
In order to enable the Oracle JDBC driver in Maven's pom.xml, please take a look on the following link:
http://stackoverflow.com/questions/1074869/find-oracle-jdbc-driver-in-maven-repository

TODO:
-----
 - RIAK, Cassandra: Make Quorum CRUD operations optional.
 - Base64 Reader in the BinaryFileInfo.
 - Switchable storage engines where it's available: RIAK, MySQL
 - Add missing DB samplers.
 - Add missing TestPlans for databases.
