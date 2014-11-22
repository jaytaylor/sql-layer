/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.sql.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;

import javax.management.Attribute;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/** Provides a JMX interface to the server in the test framework */
public class JMXInterpreter implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(JMXInterpreter.class);

    public JMXServiceURL serviceURL;
    private JmxAdapter adapter;

    void ensureConnection(String host, int port) {
        if(adapter == null) {
            adapter = new RemoteJmxAdapter(host, port);
            if(!adapter.tryOpen()) {
                LOG.debug("Couldn't connect to remote JMX adapter: {}", adapter.describeConnection());
                adapter = new LocalJmxAdapter();
                if(!adapter.tryOpen()) {
                    LOG.debug("Couldn't connect to local JMX adapter: {}", adapter.describeConnection());
                }
            }
        }
    }

    @Override
    public void close() {
        try {
            if(adapter != null) {
                adapter.close();
            }
        } catch(IOException e) {
            LOG.debug("Caught closing adapter", e);
        }
    }

    public JmxAdapter getAdapter() {
        return adapter;
    }

    public Object makeBeanCall(String host,
                               int port,
                               String objectName,
                               String method,
                               Object[] parameters,
                               String callType) throws Exception {
        ensureConnection(host, port);
        if(adapter == null) {
            throw new Exception("Can't connect");
        }
        MBeanServerConnection mbs = adapter.getConnection();
        ObjectName mxbeanName = new ObjectName(objectName);

        MBeanInfo info = mbs.getMBeanInfo(mxbeanName);
        String[] signature = null;
        if(callType.equalsIgnoreCase("method")) {
            for(MBeanOperationInfo op : info.getOperations()) {
                if(method.equalsIgnoreCase(op.getDescription())) {
                    signature = new String[op.getSignature().length];
                    for(int a = 0; a < op.getSignature().length; a++) {
                        signature[a] = op.getSignature()[a].getType();
                    }
                    break;
                }
            }
        }
        Object data = null;
        if(callType.equalsIgnoreCase("method")) {
            data = mbs.invoke(mxbeanName, method, parameters, signature);
        } else if(callType.equalsIgnoreCase("get")) {
            data = mbs.getAttribute(mxbeanName, method);
        } else {
            Attribute attr = null;
            for(int x = 0; x < info.getAttributes().length; x++) {
                if(method.equalsIgnoreCase(info.getAttributes()[x].getName())) {
                    if(info.getAttributes()[x].getType().equalsIgnoreCase(double.class.getName())) {
                        attr = new Attribute(method, new Double(String.valueOf(parameters[0])));
                    } else if(info.getAttributes()[x].getType().equalsIgnoreCase(long.class.getName())) {
                        attr = new Attribute(method, new Long(String.valueOf(parameters[0])));
                    } else if(info.getAttributes()[x].getType().equalsIgnoreCase(int.class.getName())) {
                        attr = new Attribute(method, new Integer(String.valueOf(parameters[0])));
                    } else if(info.getAttributes()[x].getType().equalsIgnoreCase(String.class.getName())) {
                        attr = new Attribute(method, String.valueOf(parameters[0]));
                    } else if(info.getAttributes()[x].getType().equalsIgnoreCase(Boolean.class.getName())) {
                        attr = new Attribute(method, Boolean.valueOf(String.valueOf(parameters[0])));
                    } else {
                        throw new Exception("Unknown Attribute type found as " + info.getAttributes()[x].getType());
                    }
                    break;
                }
            }
            mbs.setAttribute(mxbeanName, attr);
        }

        return data;
    }

    public interface JmxAdapter extends Closeable
    {
        boolean tryOpen();

        String describeConnection();

        MBeanServerConnection getConnection();
    }

    private static class RemoteJmxAdapter implements JmxAdapter
    {

        @Override
        public boolean tryOpen() {
            JMXServiceURL serviceUrl;
            //service:jmx:rmi:///jndi/rmi://localhost:8082/jmxrmi
            try {
                serviceUrl = new JMXServiceURL(urlString);
            } catch(MalformedURLException e) {
                LOG.warn("Caught opening URL: {}", urlString, e);
                return false;
            }
            try {
                connector = JMXConnectorFactory.connect(serviceUrl);
                connection = connector.getMBeanServerConnection();
            } catch(IOException e) {
                LOG.warn("Error connecting to URL: {}", serviceUrl, e);
                return false;
            }
            assert connection != null;
            return true;
        }

        @Override
        public MBeanServerConnection getConnection() {
            if(connection == null) {
                throw new IllegalStateException("not connected: " + describeConnection());
            }
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

    private static class LocalJmxAdapter implements JmxAdapter
    {
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
