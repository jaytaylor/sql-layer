REM This file is for Windows. For Unix-like systems, look in jvm.options.
REM It is important that none of the property values have spaces in them.

REM The amount of memory, in megabytes, to allocate to the JVM at startup.
REM You almost certainly want to adjust this for your environment.
REM Do not include unit suffix.
REM SET MAX_HEAP_MB=1024

REM Here we create the arguments that will get passed to the jvm when starting.

REM enable thread priorities, primarily so we can give periodic tasks
REM a lower priority to avoid interfering with client workload
SET JVM_OPTS=%JVM_OPTS% -XX:+UseThreadPriorities
REM allows lowering thread priority without being root.  see
REM http://tech.stolsvik.com/2010/01/linux-java-thread-priorities-workaround.html
SET JVM_OPTS=%JVM_OPTS% -XX:ThreadPriorityPolicy=42

REM min and max heap sizes should be set to the same value to avoid
REM stop-the-world GC pauses during resize, and so that we can lock the
REM heap in memory on startup to prevent any of it from being swapped
REM out.
SET JVM_OPTS=%JVM_OPTS% -XX:+HeapDumpOnOutOfMemoryError

REM GC logging options -- uncomment to enable
REM SET JVM_OPTS=%JVM_OPTS% -XX:+PrintGCDetails
REM SET JVM_OPTS=%JVM_OPTS% -XX:+PrintGCTimeStamps
REM SET JVM_OPTS=%JVM_OPTS% -XX:+PrintClassHistogram
REM SET JVM_OPTS=%JVM_OPTS% -XX:+PrintTenuringDistribution
REM SET JVM_OPTS=%JVM_OPTS% -XX:+PrintGCApplicationStoppedTime
REM SET JVM_OPTS=%JVM_OPTS% -Xloggc:gc.log

REM JMX: metrics and administration interface
REM 
REM add this if you're having trouble connecting:
REM SET JVM_OPTS=%JVM_OPTS% -Djava.rmi.server.hostname=<public name>
REM 
REM see 
REM http://blogs.sun.com/jmxetc/entry/troubleshooting_connection_problems_in_jconsole
REM for more on configuring JMX through firewalls, etc. (Short version:
REM get it working with no firewall first.)
SET JMX_PORT=8082
SET JVM_OPTS=%JVM_OPTS% -Dcom.sun.management.jmxremote.port=%JMX_PORT%
SET JVM_OPTS=%JVM_OPTS% -Dcom.sun.management.jmxremote.ssl=false
SET JVM_OPTS=%JVM_OPTS% -Dcom.sun.management.jmxremote.authenticate=false

SET JVM_OPTS=%JVM_OPTS% -Xrunjdwp:transport=dt_socket,address=8000,suspend=n,server=y
