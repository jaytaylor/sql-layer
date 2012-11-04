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

package com.akiban.sql.embedded;

import com.akiban.sql.server.ServerServiceRequirements;
import com.akiban.server.service.monitor.ServerMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

// TODO: Consider having an EmbeddedDriver proxy class also implements
// Driver and wraps this. It can live in a separate .jar file in a
// class loader that servlets, scripts, etc. have access to and so
// satisfy java.sql.DriverManager.isDriverAllowed().
public class JDBCDriver implements Driver, ServerMonitor {
    public static final String URL = "jdbc:default:connection";

    private final ServerServiceRequirements reqs;
    private final long startTime = System.currentTimeMillis();
    private int nconnections;

    private static final Logger logger = LoggerFactory.getLogger(JDBCDriver.class);

    protected JDBCDriver(ServerServiceRequirements reqs) {
        this.reqs = reqs;
    }

    public void register() throws SQLException {
        DriverManager.registerDriver(this);
        reqs.monitor().registerServerMonitor(this);
    }

    public void deregister() throws SQLException {
        reqs.monitor().deregisterServerMonitor(this);
        DriverManager.deregisterDriver(this);
    }

    /* Driver */

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!url.equals(URL)) return null;
        nconnections++;
        return new JDBCConnection(reqs, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.equals(URL);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }


    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    //@Override // JDK 1.7
    public java.util.logging.Logger getParentLogger() 
            throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Uses LOG4J");
    }

    /* ServerMonitor */

    @Override
    public String getServerType() {
        return JDBCConnection.SERVER_TYPE;
    }

    @Override
    public int getLocalPort() {
        return -1;
    }

    @Override
    public long getStartTimeMillis() {
        return startTime;
    }
    
    @Override
    public int getSessionCount() {
        return nconnections;
    }

}
