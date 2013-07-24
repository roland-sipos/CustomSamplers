CustomSamplers
==============

The CustomSamplers project is a fairly big extension to JMeter that 
contains custom configuration and sampler elements for different databases 
in order to measure performance characteristics of BLOB data handling for
given scenarios.

Currently the following databases are supported by configuration and sampler elements:
NoSQL:
------
 + Cassandra
 + RIAK
 + MongoDB
 + CouchDB
 + Voldemort
 - Accumulo
 - HBase
 
NewSQL:
-------
 + Drizzle
 - TokuDB

Relational:
-----------
 + PostgreSQL
 + MySQL
 - Oracle 11g
 
TODO:
-----
 - Cassandra: Check Quorum write and read, and unify the numbers for all the tests.
 - RIAK: Same as for Cassandra. Quorum writen, ONE read probably the best.
 - Extend MongoDB with Morphia POJO mapping. (??? Probably not a good idea...)
 - CouchDB - Base64 encoding flaw? What cause it?
 - Switchable storage engines where it's available: RIAK, MySQL
 - Add missing DB samplers.
 - Unify QuryHandling.
 - Unify test databases and clusters
   -> testdb deyploers to be implemented. See DrizzleDeployer.
   (Plan: )
