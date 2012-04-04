/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.pg;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
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
    private JMXConnector connector;
    private boolean debug;
    

    public JMXInterpreter(boolean debug) {
        super();
        this.debug = debug;
        // TODO Auto-generated constructor stub
    }

    void openConnection() {
        try {
            connector = JMXConnectorFactory.connect(serviceURL);
            if (debug) {
                System.out.println("Opening connection to "+serviceURL);
            }
        } catch (IOException e) {
            System.out.println(serviceURL);
            System.out.println("Error "+e.getMessage());
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
            if (connector != null) {
                connector.close();
            }
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
    
    public MBeanServerConnection setup(String host, String port) {
        openConnection(host, port);
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

    
    public Object makeBeanCall(String host, String port, String objectName, 
            String method, Object[] paramters, String callType, String setvalue) throws Exception {
        setup(host, port);
        final MBeanServerConnection mbs = connector
                .getMBeanServerConnection();
        if (mbs == null) { 
            throw new Exception("Can't connect");
        }
        ObjectName mxbeanName = null;
            mxbeanName = new ObjectName(objectName);
        
        MBeanInfo info = mbs.getMBeanInfo(mxbeanName);
        String[] signature = null;
        if (callType.equalsIgnoreCase("method")) {
            MBeanOperationInfo[] ops = info.getOperations();
            for (int x=0;x < ops.length;x++) {
                if (method.equalsIgnoreCase(ops[x].getDescription())) {
                    for (int a=0;a < ops[x].getSignature().length;a++) {
                        signature = new String[ops[x].getSignature().length];
                        signature[a] = ops[x].getSignature()[a].getType();
                    }    
                }
            }    
        }
        Object data = null;
        if (callType.equalsIgnoreCase("method")) {
            data = mbs.invoke(mxbeanName, method, paramters , signature);
        } else if (callType.equalsIgnoreCase("get")) {
            data = mbs.getAttribute(mxbeanName, method);
        } else {
            Attribute attrib = null;
            for (int x=0;x < info.getAttributes().length;x++) {
                if (method.equalsIgnoreCase(info.getAttributes()[x].getName())) {
                    if (debug) {
                        System.out.println(info.getAttributes()[x].getType()+" vs "+long.class.getName());
                    }
                    if (info.getAttributes()[x].getType().equalsIgnoreCase(long.class.getName())) {
                        attrib = new Attribute(method, new Integer(setvalue));
                    }
                    if (info.getAttributes()[x].getType().equalsIgnoreCase(String.class.getName())) {
                        attrib = new Attribute(method, new String(setvalue));
                    }
                }
            }
            mbs.setAttribute(mxbeanName, attrib);
        }
        
        return data;
    }


}
