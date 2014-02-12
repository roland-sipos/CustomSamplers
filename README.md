CustomSamplers
==============

The CustomSamplers project is a fairly big extension to the JMeter tool that 
contains custom configuration and sampler elements for different relational and
non-relation databases, in order to measure performance characteristics of binary
data handling for given workloads in the CMS Conditions environment.

Currently the following databases are supported by configuration and sampler elements:
NoSQL:
------
 + Cassandra
 + RIAK
 + MongoDB
 + CouchDB
 + Voldemort
 + Accumulo
 + HBase
 + Hypertable

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

2. Like for 1., but a little bit different: The Hypertable connectivity driver will be present in the 
installation directory of a Hypertable instance. The Thrift API is also a dependency, so you need that too
in order to connect to Hypertable with a CustomSampler test-suit.
With a basic installation these are located here:

/opt/hypertable/*version*/lib/java/hyertable-*version*.jar  # and thrift-*version*.jar

You need to add these JARs to your project's Java build path (or to your Java classpath) in order to make the 
HypertableDeployer executable from your IDE (or from command line). For the Maven build, you also need to 
manually add these artifacts to you local Maven repository. (See: 1.)

TODO:
-----
 - RIAK, Cassandra: Make Quorum CRUD operations optional.
 - Base64 Reader in the BinaryFileInfo. (still needed?)
 - ConfigElements should remove persistency objects on their own, when the test ended.
 - Switchable storage engines where it's available: RIAK, MySQL
 - Add/Correct TestPlans for databases.

