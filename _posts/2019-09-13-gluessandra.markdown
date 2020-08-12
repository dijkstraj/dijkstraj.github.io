---
layout: post
title:  "Gluessandra: reading from Cassandra in your Glue"
date:   2019-09-13 13:45:45 +0200
categories: glue cassandra spark
---

## Prepare drivers

### Download from Maven Central

We're using the unshaded version of the Cassandra connector to prevent this error: https://stackoverflow.com/a/42263362

{% highlight bash %}
version=2.4.1
mvn dependency:get \
  -DgroupId=com.datastax.spark \
  -DartifactId=spark-cassandra-connector_2.11 \
  -Dversion=${version}
mvn dependency:copy-dependencies \
  -f ~/.m2/repository/com/datastax/spark/spark-cassandra-connector-unshaded_2.11/${version}/spark-cassandra-connector-unshaded_2.11-${version}.pom \
  -DoutputDirectory=./spark-connector-libs
cp ~/.m2/repository/com/datastax/spark/spark-cassandra-connector-unshaded_2.11/${version}/spark-cassandra-connector-unshaded_2.11-${version}.jar \
  ./spark-connector-libs/
{% endhighlight %}

### Upload to an S3 bucket

Glue jobs can load dependencies from an S3 bucket,
so we park the Spark connector there.

{% highlight bash %}
aws s3 cp ./spark-connector-libs/ s3://some-bucket/drivers/ --recursive --exclude '*' \
  --include 'cassandra-driver-*.jar' \
  --include 'guava-*.jar' \
  --include 'spark-cassandra-connector-*.jar'
{% endhighlight %}

## Create a job

Set the _"Dependent jars path"_ of your job to: `s3://some-bucket/drivers/spark-cassandra-connector/guava-16.0.1.jar,s3://some-bucket/drivers/spark-cassandra-connector/cassandra-driver-core-3.1.4.jar,s3://some-bucket/drivers/spark-cassandra-connector/cassandra-driver-mapping-3.1.4.jar,s3://some-bucket/drivers/spark-cassandra-connector/spark-cassandra-connector-unshaded_2.11-2.4.1.jar`

### Example script in Scala

{% highlight scala %}
%spark

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import org.apache.spark.sql.cassandra._
import com.amazonaws.services.glue.{DynamicRecord, GlueContext}
import org.apache.spark.SparkContext

val sc: SparkContext = SparkContext.getOrCreate()
val glueContext: GlueContext = new GlueContext(sc)

val connector = CassandraConnector(sc.getConf
  .set("spark.cassandra.connection.host", "1.2.3.4")
  .set("spark.cassandra.auth.username", "cassandra")
  .set("spark.cassandra.auth.password", "cassandra")
)
implicit val c = connector
val rdd = sc.cassandraTable("my_keyspace", "my_table")
println(rdd.first)
{% endhighlight %}

### Example script in Python

{% highlight python %}
%pyspark

from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

load_options = {
    "table": "my_table",
    "keyspace": "my_keyspace",
    "spark.cassandra.connection.host": "1.2.3.4",
    "spark.cassandra.auth.username": "cassandra",
    "spark.cassandra.auth.password": "cassandra"
}
spark.read.format('org.apache.spark.sql.cassandra')
    .options(**load_options)
    .load()
    .show()
{% endhighlight %}