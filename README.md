LogSpark
=================

Log file processor example project using Spark. This could be extended to build a more complex log processor or machine learning on big data on Spark (there are some samples from Spark source code).
Data can come from HDFS, Stream (in Spark's new version), S3, local storage, etc according to whatever Spark supports.  At the point of testing, this works with Spark 0.6.1.

This project depends on Spark jar from https://github.com/mesos/spark

1. Compile: sbt/sbt compile
2. Jar : sbt/sbt package
3. Clean: sbt/sbt clean
4. Eclipse project: sbt/sbt eclipse
