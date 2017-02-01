FROM gettyimages/spark:1.6.0-hadoop-2.6
MAINTAINER Daniel Imberman
#WORKDIR $SPARK_HOME
ADD ./target/scala-2.10/DollarShaveTest-assembly-1.0.jar /app/CodingExample.jar
ADD ./data/pageViews.csv /app/input.csv

CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]
