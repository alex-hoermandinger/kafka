SET script_dir=%~dp0
SET script_dir2=%script_dir:~0,-1%

SET basedir=D:\confluent\confluent-3.3.0
SET java="%JAVA_HOME%"\bin\java.exe

%java% -Xmx512M -Xms512M -server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true -Xloggc:%basedir%/logs/zookeeper-gc.log -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dkafka.logs.dir=%basedir%/logs -Dlog4j.configuration=file:%basedir%/etc/kafka/log4j.properties -cp .;%basedir%/share/java/kafka/*;%basedir%/share/java/confluent-support-metrics/* org.apache.zookeeper.server.quorum.QuorumPeerMain %basedir%/etc/kafka/zookeeper.properties

