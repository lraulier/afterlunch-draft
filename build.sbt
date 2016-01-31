name := "afterlunch-draft"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++=Seq("org.apache.spark" %% "spark-sql" % "1.4.1","org.apache.hadoop" % "hadoop-client" % "2.2.0")
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
