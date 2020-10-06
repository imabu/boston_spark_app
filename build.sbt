resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

name := "boston_spark_app"

version := "0.0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % "2.4.4" % "provided")
