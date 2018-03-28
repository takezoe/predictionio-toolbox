name := "predictionio-toolbox"

organization := "com.github.takezoe"

version := "0.0.1"

scalaVersion := "2.11.8"

val pioVersion = "0.12.1"
val sparkVersion = "2.1.1"
val elasticsearchVersion = "5.5.2"
val json4sVersion = "3.2.11"
val hbaseVersion = "0.98.5-hadoop2"
val hadoopVersion = "2.7.3"

libraryDependencies ++= Seq(
  "org.apache.predictionio"  %% "apache-predictionio-common"             % pioVersion,
  "org.apache.predictionio"  %% "apache-predictionio-core"               % pioVersion,
  "org.apache.predictionio"  %% "apache-predictionio-data"               % pioVersion,
  "org.apache.predictionio"  %% "apache-predictionio-data-jdbc"          % pioVersion,
  "org.apache.predictionio"  %% "apache-predictionio-data-elasticsearch" % pioVersion,
  "org.apache.predictionio"  %% "apache-predictionio-data-hbase"         % pioVersion,
  "org.postgresql"           %  "postgresql"                             % "42.1.4",
  "mysql"                    %  "mysql-connector-java"                   % "5.1.46",
  "org.clapper"              %% "grizzled-slf4j"                         % "1.3.2",
  "com.github.nscala-time"   %% "nscala-time"                            % "2.18.0",
  "org.json4s"               %% "json4s-native"                          % json4sVersion,
  "org.json4s"               %% "json4s-ext"                             % json4sVersion,
  "org.scalikejdbc"          %% "scalikejdbc"                            % "3.1.0",
  "org.elasticsearch.client" %  "rest"                                   % elasticsearchVersion,
  "org.elasticsearch"        %% "elasticsearch-spark-20"                 % elasticsearchVersion exclude("org.apache.spark", "*"),
  "org.apache.hbase"         %  "hbase-common"                           % hbaseVersion,
  "org.apache.hbase"         %  "hbase-client"                           % hbaseVersion exclude("org.apache.zookeeper", "zookeeper"),
  "org.apache.hbase"         %  "hbase-server"                           % hbaseVersion
    exclude("org.apache.hbase"    , "hbase-client")
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("javax.servlet"       , "servlet-api")
    exclude("org.mortbay.jetty"   , "servlet-api-2.5")
    exclude("org.mortbay.jetty"   , "jsp-api-2.1")
    exclude("org.mortbay.jetty"   , "jsp-2.1"),
  "org.apache.spark"         %% "spark-core"                             % sparkVersion % "provided"
)