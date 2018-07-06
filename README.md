# MrProject
map reduce project framework

mvn install:install-file -Dfile=./hbase-client-2.0.0.jar -DgroupId=org.apache.hbase -DartifactId=hbase-client -Dversion=2.0.0 -Dpackaging=jar \
& mvn install:install-file -Dfile=./hbase-server-2.0.0.jar -DgroupId=org.apache.hbase -DartifactId=hbase-server -Dversion=2.0.0 -Dpackaging=jar \
& mvn install:install-file -Dfile=./hbase-mapreduce-2.0.0.jar -DgroupId=org.apache.hbase -DartifactId=hbase-mapreduce -Dversion=2.0.0 -Dpackaging=jar \
& mvn install:install-file -Dfile=./hbase-common-2.0.0.jar -DgroupId=org.apache.hbase -DartifactId=hbase-common -Dversion=2.0.0 -Dpackaging=jar
& mvn install:install-file -Dfile=./hadoop-mapreduce-client-core-3.1.0.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-mapreduce-client-core -Dversion=3.1.0 -Dpackaging=jar
& mvn install:install-file -Dfile=./hadoop-hdfs-3.1.0.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-hdfs -Dversion=3.1.0 -Dpackaging=jar