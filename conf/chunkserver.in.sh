

chunkserver_home=`dirname $0`/..

# This can be the path to a jar file, or a directory containing the 
# compiled classes. NOTE: This isn't needed by the startup script,
# it's just used here in constructing the classpath.
chunkserver_bin=$chunkserver_home/target/classes

# JAVA_HOME can optionally be set here
#JAVA_HOME=/usr/local/jdk6

# The java classpath (required)
CLASSPATH=$CHUNKSERVER_CONF:$chunkserver_bin

for jar in $chunkserver_home/target/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

# Arguments to pass to the JVM
JVM_OPTS=" \
        -ea \
        -Xnoagent \
        -Xmx1G \
        -Djava.compiler=NONE \
        -Xrunjdwp:transport=dt_socket,address=8000,suspend=n,server=y"

# the JAR file we use to launch the chunk server
JAR_FILE="../target/akiban-cserver-1.0-SNAPSHOT-jar-with-dependencies.jar"

# the config file to use for the chunk server
CONFIG_FILE="../conf/cserver.properties"
