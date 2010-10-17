# /* <GENERIC-HEADER - BEGIN>
#  *
#  * $(COMPANY) $(COPYRIGHT)
#  *
#  * Created on: Nov, 20, 2009
#  * Created by: Thomas Hazel
#  *
#  * </GENERIC-HEADER - END> */

#
# Install jars in your local repository
#
$ mvn clean source:jar install

#
# Creates a jar file that can be put into an application war file
#
$ mvn clean package
