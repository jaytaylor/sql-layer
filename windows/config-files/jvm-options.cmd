REM
REM END USER LICENSE AGREEMENT (“EULA”)
REM
REM READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
REM http://www.akiban.com/licensing/20110913
REM
REM BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
REM ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
REM AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
REM
REM IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
REM THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
REM NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
REM YOUR INITIAL PURCHASE.
REM
REM IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
REM CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
REM FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
REM LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
REM BY SUCH AUTHORIZED PERSONNEL.
REM
REM IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
REM USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
REM PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
REM

REM This file is for Windows. For Unix-like systems, look in jvm.options.
REM It is important that none of the property values have spaces in them.

REM The amount of memory to allocate to the JVM at startup, you almost
REM certainly want to adjust this for your environment.
REM SET MAX_HEAP_SIZE=1024M

REM Here we create the arguments that will get passed to the jvm when
REM starting the akiban server.

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

REM GC tuning options
SET JVM_OPTS=%JVM_OPTS% -XX:+UseParNewGC

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
