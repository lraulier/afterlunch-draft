import AssemblyKeys._

name := "afterlunch-draft"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++=Seq("org.apache.spark" %% "spark-core" % "1.4.1","org.apache.hadoop" % "hadoop-client" % "2.2.0")

assemblySettings

jarName in assembly := "afterlunch-draft.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

