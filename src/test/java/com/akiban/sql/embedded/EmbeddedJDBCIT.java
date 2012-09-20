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

import com.akiban.server.test.it.ITBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;

import java.sql.*;

import com.akiban.server.service.config.Property;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import java.util.Collection;

public class EmbeddedJDBCIT extends ITBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
            .bindAndRequire(EmbeddedJDBCService.class, EmbeddedJDBCServiceImpl.class);
    }

    @Override
    protected Collection<Property> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    public static final String SCHEMA_NAME = "test";
    public static final String CONNECTION_URL = "jdbc:default:connection";
    
    @Before
    public void loadDB() throws Exception {
        int cid = createTable(SCHEMA_NAME, "c",
                              "cid int primary key not null",
                              "name varchar(16) not null");
        int oid = createTable(SCHEMA_NAME, "o",
                              "oid int primary key not null",
                              "cid int not null",
                              "grouping foreign key(cid) references c(cid)",
                              "order_date date not null");
        writeRow(cid, 1, "Smith");
        writeRow(oid, 101, 1, 2012 * 512 + 1 * 32 + 31);
        writeRow(cid, 2, "Jones");
        writeRow(oid, 201, 2, 2012 * 512 + 4 * 32 + 1);
    }
    
    @Test
    public void testSimple() throws Exception {
        Connection conn = DriverManager.getConnection(CONNECTION_URL, SCHEMA_NAME, "");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT name FROM c WHERE cid = 1");
        assertTrue("has first row", rs.next());
        assertEquals("result value", "Smith", rs.getString(1));
        assertFalse("has more rows", rs.next());
        rs.close();
        stmt.close();
        conn.close();
    }

}
