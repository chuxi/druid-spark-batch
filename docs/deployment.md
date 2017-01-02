## spark hive batch deployment

1. requires druid 0.9.1.1 with overload, middleManager, S3 or HDFS as deep storage.
2. requires spark >= 2.0.2

## steps
1. `sbt package` => druid-spark-batch_2.11-0.9.1.jar
2. make extension: `druid-spark-batch`. mkdir druid/extensions/druid-spark-batch
3. copy three jars to druid-spark-batch: druid-spark-batch_2.11-0.9.1.jar, scala-library-2.11.8.jar, jackson-module-scala_2.11-2.4.5.jar
4. deploy spark to middleManager node, set SPARK_HOME env
5. mkdir & cd druid/spark-dependencies/spark-hive_2.11, make symlink for spark-hive's version: ln -s spark/jars 2.0.2
6. cd /apps/svr/spark/jars, rm guice-3.0.jar & rm guice-servlet-3.0.jar, conflict with guice-4.1.jar
7. copy core-site.xml, hdfs-site.xml, yarn-site.xml, hive-site.xml, jets3t.properties
8. change common.runtime.properties: `druid.extensions.loadList=["druid-spark-batch", ...]`, `druid.extensions.hadoopDependenciesDir=/path/to/druid/spark-dependencies`, do not include any kafka extensions.
9. start overload and middleManager, submit tasks