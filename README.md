# Uniserve

This is a code release for our 2022 NSDI paper [Data-Parallel Actors: A Programming Model for Scalable Query Serving Systems](http://petereliaskraft.net/res/uniserve.pdf).  This is the code for the Uniserve runtime described in that paper.  Uniserve requires a ZooKeeper server local to each coordinator running on port 2181.  Additionally, certain features rely on S3 and require AWS authentication.
