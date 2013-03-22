/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.sql.pg;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import javax.management.Attribute;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/* Provides a JMX interface to the server in the test framework  */
public class JMXInterpreter {

    public JMXServiceURL serviceURL;
    private JmxAdapter adapter;
    private boolean debug;

    public JMXInterpreter(boolean debug) {
        this.debug = debug;
    }

    void ensureConnection(String host, int port) {
        if (adapter == null) {
            adapter = new RemoteJmxAdapter(host, port);
            if (!adapter.tryOpen()) {
                if (debug)
                    System.out.println("Couldn't connect to remote JMX adapter: " + adapter.describeConnection());
                adapter = new LocalJmxAdapter();
                if ((!adapter.tryOpen()) && debug)
                    System.out.println("Couldn't connect to local JMX adapter: " + adapter.describeConnection());
            }
        }
    }

    public void close() {
        try {
            if (adapter != null) {
                adapter.close();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public JmxAdapter getAdapter() {
        return adapter;
    }

    /* Used for generating documentation for the wiki */
    public void getInfo(MBeanServerConnection mbsc) {
        echo("Domains:");
        String domains[] = null;
        try {
            domains = mbsc.getDomains();

            Arrays.sort(domains);
            for (String domain : domains) {
                echo("\tDomain = " + domain);
            }

            echo("MBeanServer default domain = " + mbsc.getDefaultDomain());

            echo("MBean count = " + mbsc.getMBeanCount());
            echo("Query MBeanServer MBeans:");
            Set<ObjectName> names = null;
            names = new TreeSet<>(mbsc.queryNames(null, null));
            for (ObjectName name : names) {
                echo("----------------------------");
                echo("* ObjectName = " + name + " * ");
                printMBeanInfo(mbsc, name);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void echo(String string) {
        System.out.println(System.getProperty("line.separator") + string);

    }

    /* Used for generating documentation for the wiki */
    private void printMBeanInfo(MBeanServerConnection mbs,
            ObjectName mbeanObjectName) {
        MBeanInfo info = null;
        try {
            info = mbs.getMBeanInfo(mbeanObjectName);
        } catch (Exception e) {
            echo("!!! Could not get MBeanInfo object for " + mbeanObjectName
                    + " !!!");
            e.printStackTrace();
            return;
        }

        MBeanAttributeInfo[] attrInfo = info.getAttributes();

        if (attrInfo.length > 0) {
            echo("|| Attribute || Type || Readable || Writeable || Desc || ");
            for (int i = 0; i < attrInfo.length; i++) {
                echo(" | " + attrInfo[i].getName() + " | "
                        + attrInfo[i].getType() + " | "
                        + attrInfo[i].isReadable() + " | "
                        + attrInfo[i].isWritable() + " | "
                        + attrInfo[i].getDescription() + " | ");
                //echo("    DESCR:        " + attrInfo[i].getDescription());
            }
        }

        //MBeanConstructorInfo[] ctors = info.getConstructors();
        MBeanNotificationInfo[] nots = info.getNotifications();
        MBeanOperationInfo[] opInfo = info.getOperations();

        if (nots.length > 0) {
            System.out.println("*Notifications:*\n");
            for (int o = 0; o < nots.length; o++) {
                System.out.println("| " + nots[o].getDescription() + " | ");
            }
        }

        System.out.println("* Operations: *");
        for (int o = 0; o < opInfo.length; o++) {
            MBeanOperationInfo op = opInfo[o];

            String returnType = op.getReturnType();
            String opName = op.getName();
            System.out.print(" | " + returnType + " | " + opName + "(");

            MBeanParameterInfo[] params = op.getSignature();
            for (int p = 0; p < params.length; p++) {
                MBeanParameterInfo paramInfo = params[p];

                String pname = paramInfo.getName();
                String type = paramInfo.getType();

                if (pname.equals(type)) {
                    System.out.print(type);
                } else {
                    System.out.print(type + " " + mbeanObjectName);
                }

                if (p < params.length - 1) {
                    System.out.print(", ");
                }
            }
            System.out.println(") | ");
        }
    }

    
    public Object makeBeanCall(String host, int port, String objectName,
            String method, Object[] parameters, String callType) throws Exception {
        ensureConnection(host, port);
        if (adapter == null) {
            throw new Exception("Can't connect");
        }
        MBeanServerConnection mbs = adapter.getConnection();
        ObjectName mxbeanName = null;
            mxbeanName = new ObjectName(objectName);
        
        MBeanInfo info = mbs.getMBeanInfo(mxbeanName);
        String[] signature = null;
        if (callType.equalsIgnoreCase("method")) {
            MBeanOperationInfo[] ops = info.getOperations();
            for (int x=0;x < ops.length;x++) {
                if (method.equalsIgnoreCase(ops[x].getDescription())) {
                    signature = new String[ops[x].getSignature().length];
                    for (int a=0;a < ops[x].getSignature().length;a++) {
                        signature[a] = ops[x].getSignature()[a].getType();
                    }
                    break;
                }
            }    
        }
        Object data = null;
        if (callType.equalsIgnoreCase("method")) {
            data = mbs.invoke(mxbeanName, method, parameters , signature);
        } else if (callType.equalsIgnoreCase("get")) {
            data = mbs.getAttribute(mxbeanName, method);
        } else {
            Attribute attrib = null;
            for (int x=0;x < info.getAttributes().length;x++) {
                if (method.equalsIgnoreCase(info.getAttributes()[x].getName())) {
                    if (info.getAttributes()[x].getType().equalsIgnoreCase(double.class.getName())) {
                        attrib = new Attribute(method, new Double(String.valueOf(parameters[0])));
                    } else if (info.getAttributes()[x].getType().equalsIgnoreCase(long.class.getName())) {
                        attrib = new Attribute(method, new Long(String.valueOf(parameters[0])));
                    } else if (info.getAttributes()[x].getType().equalsIgnoreCase(int.class.getName())) {
                        attrib = new Attribute(method, new Integer(String.valueOf(parameters[0])));
                    } else if (info.getAttributes()[x].getType().equalsIgnoreCase(String.class.getName())) {
                        attrib = new Attribute(method, String.valueOf(parameters[0]));
                    } else if (info.getAttributes()[x].getType().equalsIgnoreCase(Boolean.class.getName())) {
                        attrib = new Attribute(method, new Boolean(String.valueOf(parameters[0])));
                    } else {
                        throw new Exception("Unknown Attribute type found as "+info.getAttributes()[x].getType());
                    }
                    break;
                }
            }
            mbs.setAttribute(mxbeanName, attrib);
        }
        
        return data;
    }

    public interface JmxAdapter extends Closeable {
        boolean tryOpen();
        String describeConnection();
        MBeanServerConnection getConnection();
    }

    private static class RemoteJmxAdapter implements JmxAdapter {

        @Override
        public boolean tryOpen() {
            JMXServiceURL serviceUrl;
            //service:jmx:rmi:///jndi/rmi://localhost:8082/jmxrmi
            try {
                serviceUrl = new JMXServiceURL(urlString);
            }
            catch (MalformedURLException e) {
                System.err.println("Malformed JMX connection string: " + urlString);
                return false;
            }
            try {
                connector = JMXConnectorFactory.connect(serviceUrl);
                connection = connector.getMBeanServerConnection();
            }
            catch (IOException e) {
                return false;
            }
            assert connection != null;
            return true;
        }

        @Override
        public MBeanServerConnection getConnection() {
            if (connection == null)
                throw new IllegalStateException("not connected: " + describeConnection());
            return connection;
        }

        @Override
        public String describeConnection() {
            return urlString;
        }

        @Override
        public void close() throws IOException {
            connector.close();
        }

        public RemoteJmxAdapter(String host, int port) {
            urlString = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi";
        }

        private final String urlString;
        private JMXConnector connector;
        private MBeanServerConnection connection;
    }

    private static class LocalJmxAdapter implements JmxAdapter {
        @Override
        public boolean tryOpen() {
            return true;
        }

        @Override
        public String describeConnection() {
            return "local VM connection";
        }

        @Override
        public MBeanServerConnection getConnection() {
            return ManagementFactory.getPlatformMBeanServer();
        }

        @Override
        public void close() {
            // nothing to do
        }
    }
}
