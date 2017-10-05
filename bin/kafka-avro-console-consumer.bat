SET script_dir=%~dp0
SET script_dir2=%script_dir:~0,-1%

SET basedir=D:\confluent\confluent-3.3.0
SET java="%JAVA_HOME%"\bin\java.exe

#SET topic=connect-test
SET topic=test-sqlite-jdbc-accounts

%java% -Xmx512M -server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dschema-registry.log.dir=%basedir%/logs -Dlog4j.configuration=file:%basedir%/etc/schema-registry/log4j.properties -cp %basedir%/share/java/confluent-common/*;%basedir%/share/java/rest-utils/*;%basedir%/share/java/schema-registry/*;%basedir%/share/java/kafka-connect-storage-common/* kafka.tools.ConsoleConsumer --formatter io.confluent.kafka.formatter.AvroMessageFormatter --property schema.registry.url=http://localhost:8081 --bootstrap-server localhost:9092 --topic %topic% --from-beginning




