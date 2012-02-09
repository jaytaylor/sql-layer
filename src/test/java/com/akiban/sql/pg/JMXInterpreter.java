/**
 * Copyright (C) 2011test Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import javax.management.JMX;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import com.akiban.server.manage.ManageMXBean;
import com.akiban.server.manage.ManageMXBeanImpl;
import com.akiban.server.store.statistics.IndexStatisticsMXBean;

/* Provides a JMX interface to the server in the test framework  */
public class JMXInterpreter {

    private JMXServiceURL serviceURL;
    private JMXConnector connector;

    void openConnection() {
        try {
            connector = JMXConnectorFactory.connect(serviceURL);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    void openConnection(String host, String port) {
        try {
            setServiceURL(host, port);
            openConnection();
        } catch (MalformedURLException e) {
            System.out.println(e.getMessage());
        }
    }

    public String remoteRuntime() throws IOException {
        //Get an MBeanServerConnection on the remote VM.
        final MBeanServerConnection remote = connector
                .getMBeanServerConnection();

        final RuntimeMXBean remoteRuntime = ManagementFactory
                .newPlatformMXBeanProxy(remote,
                        ManagementFactory.RUNTIME_MXBEAN_NAME,
                        RuntimeMXBean.class);

        return "Target VM is: " + remoteRuntime.getName()
                + System.getProperty("line.separator") + "Started since: "
                + remoteRuntime.getUptime()
                + System.getProperty("line.separator") + "With Classpath: "
                + remoteRuntime.getClassPath()
                + System.getProperty("line.separator") + "And args: "
                + remoteRuntime.getInputArguments();
    }

    public void close() {
        try {
            connector.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public MBeanServerConnection setup(JMXConnector connector) {
        MBeanServerConnection mbsc = null;
        try {
            mbsc = connector.getMBeanServerConnection();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return mbsc;
    }

    public JMXServiceURL getServiceURL() {
        return serviceURL;
    }

    //service:jmx:rmi:///jndi/rmi://localhost:8082/jmxrmi
    public void setServiceURL(String host, String port)
            throws MalformedURLException {
        this.serviceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://"
                + host + ":" + port + "/jmxrmi");
    }

    public void setServiceURL(JMXServiceURL serviceURL) {
        this.serviceURL = serviceURL;
    }

    public JMXConnector getConnector() {
        return connector;
    }

    public void setConnector(JMXConnector connector) {
        this.connector = connector;
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
            names = new TreeSet<ObjectName>(mbsc.queryNames(null, null));
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
                    System.out.print(',');
                }
            }
            System.out.println(") | ");
        }
    }

    /*
     * Actual Bean implementations 
     */
    public ManageMXBean getAkServer(JMXConnector connector) {
        MBeanServerConnection mbsc = setup(connector);
        ManageMXBean mxbeanProxy = null;

        ObjectName mxbeanName = null;
        try {
            mxbeanName = new ObjectName(ManageMXBeanImpl.BEAN_NAME);
        } catch (MalformedObjectNameException e) {
            System.out.println(e.getMessage());
        } catch (NullPointerException e) {
            System.out.println(e.getMessage());
        }
        mxbeanProxy = (ManageMXBean) JMX.newMXBeanProxy(mbsc, mxbeanName,
                ManageMXBean.class);

        return mxbeanProxy;
    }

    public IndexStatisticsMXBean getIndexStatisticsMXBean(JMXConnector connector) {
        MBeanServerConnection mbsc = setup(connector);
        IndexStatisticsMXBean mxbeanProxy = null;
        try {
            ObjectName mxbeanName = new ObjectName(
                    "com.akiban:type=IndexStatistics");
            mxbeanProxy = JMX.newMXBeanProxy(mbsc, mxbeanName,
                    IndexStatisticsMXBean.class);

        } catch (MalformedObjectNameException e) {
            System.out.println(e.getMessage());
        }
        return (IndexStatisticsMXBean) mxbeanProxy;
    }

}
