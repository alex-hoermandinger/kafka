SET script_dir=%~dp0
SET script_dir2=%script_dir:~0,-1%

SET basedir=D:\confluent\confluent-3.3.0
SET java="%JAVA_HOME%"\bin\java.exe
REM SET connect_config=%basedir%/etc/kafka/connect-distributed.properties

SET connect_config=%basedir%/etc/schema-registry/connect-avro-distributed.properties

%java% -Xmx256M -server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dkafka.logs.dir=%basedir%/logs -Dlog4j.configuration=file:%basedir%/etc/kafka/connect-log4j.properties -cp %basedir%/share/java/kafka/*;%basedir%/share/java/confluent-common/*;%basedir%/share/java/kafka-serde-tools/*;%basedir%/share/java/kafka-connect-elasticsearch/*;%basedir%/share/java/kafka-connect-hdfs/*;%basedir%/share/java/kafka-connect-jdbc/*;%basedir%/share/java/kafka-connect-s3/*;%basedir%/share/java/kafka-connect-storage-common/*;%basedir%/share/java/kafka/*;%basedir%/share/java/confluent-support-metrics/* org.apache.kafka.connect.cli.ConnectDistributed %connect_config%



