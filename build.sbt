name := "SpatialProject"
 
version := "1.0"
 
scalaVersion := "2.11.6"
 
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-mllib" % "2.4.0"  
)
