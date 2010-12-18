

akiban_home=`dirname $0`/..

# This can be the path to a jar file, or a directory containing the 
# compiled classes. NOTE: This isn't needed by the startup script,
# it's just used here in constructing the classpath.
akiban_bin=$akiban_home/target/classes

# the config file to use for the chunk server
CONFIG_DIR="${akiban_home}/conf"

# default AKIBAN_CONF dir
AKIBAN_CONF=$CONFIG_DIR

# JAVA_HOME can optionally be set here
#JAVA_HOME=/usr/local/jdk6

# the JAR file we use to launch the akiban server
JAR_FILE="${akiban_home}/target/akiban-cserver-0.0.2-SNAPSHOT-jar-with-dependencies.jar"

