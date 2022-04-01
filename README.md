# Uniserve

This is a code release for our 2022 NSDI paper [Data-Parallel Actors: A Programming Model for Scalable Query Serving Systems](http://petereliaskraft.net/res/uniserve.pdf).  This is the code for the Uniserve runtime described in that paper.  

To compile and run all unit tests:

    mvn package

For all unit tests to run successfully, Uniserve requires an instance of ZooKeeper running locally on port 2181.
Uniserve also requires access to S3 and requires a bucket named "uniserve-bucket."