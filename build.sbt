name := "afterlunch-draft"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++=Seq("org.apache.spark" %% "spark-core" % "1.4.1","org.apache.hadoop" % "hadoop-client" % "2.2.0")
libraryDependencies ++=Seq("com.typesafe.slick" %% "slick" % "3.0.0", "org.slf4j" % "slf4j-nop" % "1.6.4")

