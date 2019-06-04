name := "SpatialProject"
 
version := "1.0"
 
scalaVersion := "2.11.6"
 
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-mllib" % "2.4.0",
  "org.locationtech.jts" % "jts-core" % "1.16.0",
  "org.locationtech.jts.io" % "jts-io-common" % "1.16.0",
//  "org.locationtech.jts.io" % "jts-io" % "1.16.0",
  "org.locationtech.jts" % "jts" % "1.16.0",
  "org.locationtech.jts" % "jts-modules" % "1.16.0",
  "junit" % "junit" % "3.8.1",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0",
  "com.madhukaraphatak" %% "java-sizeof" % "0.1"
)
