REM
REM This file is for Windows. For Unix-like systems, look in jvm.options.
REM
REM This file is loaded into the environment before starting the SQL Layer.
REM The value of JVM_OPTS will be passed as-is on the Java command line.
REM
REM It is important that none of the property values have spaces in them.
REM

REM Memory to dedicated to the SQL Layer process. Used for both Xmx and Xms.
SET MAX_HEAP_SIZE=512M

REM Assertions
SET JVM_OPTS=%JVM_OPTS% -ea

REM Set min and max at the same time to avoid resizing the heap while running.
SET JVM_OPTS=%JVM_OPTS% -Xms%MAX_HEAP_SIZE%
SET JVM_OPTS=%JVM_OPTS% -Xmx%MAX_HEAP_SIZE%

REM Prefer IPv4 even if IPv6 is available.
SET JVM_OPTS=%JVM_OPTS% -Djava.net.preferIPv4Stack=true

REM Heap dumps, working directory by default
SET JVM_OPTS=%JVM_OPTS% -XX:+HeapDumpOnOutOfMemoryError
REM SET JVM_OPTS=%JVM_OPTS% -XX:HeapDumpPath=%TEMP%

REM JMX
SET JVM_OPTS=%JVM_OPTS% -Dcom.sun.management.jmxremote.port=8082
SET JVM_OPTS=%JVM_OPTS% -Dcom.sun.management.jmxremote.ssl=false
SET JVM_OPTS=%JVM_OPTS% -Dcom.sun.management.jmxremote.authenticate=false

REM Debugging
SET JVM_OPTS=%JVM_OPTS% -Xrunjdwp:transport=dt_socket,address=8000,suspend=n,server=y

