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
1. In order to enable the Oracle JDBC driver in Maven's pom.xml, please take a look on the following link:
http://stackoverflow.com/questions/1074869/find-oracle-jdbc-driver-in-maven-repository

2. Like for 1., but a bit different: The Hypertable connectivity driver will be present in the 
Hypertable directory, after the installation.
With the basic setup it's located here: /opt/hypertable/*version*/lib/java/hyertable-*version*.jar
To add it to you local Maven repository do:
mvn install:install-file -Dfile=hypertable-0.9.7.12.jar -DgroupId=org.hypertable -DartifactId=hypertable -Dversion=0.9.7.12 -Dpaaging=jar


TODO:
-----
 - RIAK, Cassandra: Make Quorum CRUD operations optional.
 - Base64 Reader in the BinaryFileInfo.
 - Extend the QueryHandler interface with tearDown() for closing persistent objects.
 - Switchable storage engines where it's available: RIAK, MySQL
 - Add missing DB samplers.
 - Add missing TestPlans for databases.
