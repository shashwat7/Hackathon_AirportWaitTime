name := "AirportWaitTime"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.5"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.2" % "provided"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.6.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2"

libraryDependencies += "org.apache.spark" %% "spark-catalyst" % "1.6.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming_2.10
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka_2.10
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.2"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.8.2.0"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.0"

libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.5.0"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"


