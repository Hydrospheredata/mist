## Logger

You can use any logger on you project, for configure them, you should add appenders in logback.xml 

below you can see an example log to console, log4j and fluentd

```
<configuration>

 <appender name="sout" class="ch.qos.logback.core.ConsoleAppender">
     <layout class="ch.qos.logback.classic.PatternLayout">
         <Pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</Pattern>
     </layout>
 </appender>

 <appender name="log4j" class="ch.qos.logback.core.FileAppender">
     <file>./logs/log_log4j.log</file>
     <layout class="ch.qos.logback.classic.PatternLayout">
         <Pattern>%date %level [%thread] %logger{10} [%file:%line] %msg%n</Pattern>
     </layout>
 </appender>

 <appender name="FLUENT" class="eu.inn.fluentd.FluentdAppender">
    <tag>my.mist.project</tag>
    <remoteHost>localhost</remoteHost>
    <port>24224</port>
  </appender>

 <root level="INFO">
     <appender-ref ref="sout" />
     <appender-ref ref="log4j" />
     <appender-ref ref="FLUENT"/>
 </root>

</configuration>
```

If you use log4j and SparkVersion erlier 2.0.0, you so will need create log4j.properties.
   
```
# Set everything to be logged to the console
log4j.rootCategory=DEBUG, file
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

log4j.appender.file=org.apache.log4j.FileAppender
log4j.appender.file.File=./logs/mist.log
log4j.appender.file.append=true
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n

# Settings to quiet third party logs that are too verbose
log4j.logger.org.spark-project.jetty=WARN
log4j.logger.org.spark-project.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR

# SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs in SparkSQL with Hive support
log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR
```
 
