CustomSamplers
==============

The CustomSamplers project is an extension for JMeter that contains custom configuration and sampler elements for different relational and NoSQL databases, in order to measure performance characteristics of binary data handling simulated workloads that are similar to the CERN CMS Conditions access.

Currently the following databases are supported by configuration and sampler elements (obsolete indicates missing/incorrect parts):
NoSQL:
------
 + Accumulo (obsolete)
 + Cassandra
 + CouchDB
 + HBase
 + Hypertable (obsolete)
 + MongoDB
 + RIAK
 + Voldemort (obsolete)

Relational:
-----------
 + PostgreSQL
 + MySQL
 + Oracle

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

