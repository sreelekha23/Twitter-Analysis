name := "untitled1"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.1"

libraryDependencies += "oauth.signpost" % "signpost-core" % "1.2"
libraryDependencies += "oauth.signpost" % "signpost-commonshttp4" % "1.2"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.2"
libraryDependencies += "org.apache.tomcat" % "tomcat-catalina" % "7.0.25"

/*
libraryDependencies ++= Seq(
  "oauth.signpost" % "signpost-core" % "1.2",
  "oauth.signpost" % "signpost-commonshttp4" % "1.2",
  "org.apache.httpcomponents" % "httpclient" % "4.2"
)*/
