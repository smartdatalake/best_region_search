
version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.httpcomponents" % "httpclient" % "4.5",
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-mllib" % "2.4.0",
  "org.locationtech.jts" % "jts-core" % "1.16.0",
  "org.locationtech.jts.io" % "jts-io-common" % "1.16.0",
  // "org.locationtech.jts.io" % "jts-io" % "1.16.0",
  "org.locationtech.jts" % "jts" % "1.16.0",
  "org.locationtech.jts" % "jts-modules" % "1.16.0",
  "junit" % "junit" % "3.8.1",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0",
  "com.madhukaraphatak" %% "java-sizeof" % "0.1",
  "org.scalatra" %% "scalatra-json" % "2.4.0.RC1",
  "org.apache.spark" %% "spark-core" % "2.0.2",
  "com.sparkjava" % "spark-core" % "2.9.2",
  "org.jsoup" % "jsoup" % "1.11.3"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "BRS_REST_API.jar"