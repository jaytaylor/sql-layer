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

package com.akiban.server.service.externaldata;

import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.test.it.ITBase;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class ExternalDataServiceIT extends ITBase
{
    public static final String SCHEMA = "test";

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(ExternalDataService.class, ExternalDataServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Before
    public void createData() {
        int c = createTable(SCHEMA, "c",
                            "cid INT PRIMARY KEY NOT NULL",
                            "name VARCHAR(32)");
        int o = createTable(SCHEMA, "o",
                            "oid INT PRIMARY KEY NOT NULL",
                            "cid INT NOT NULL",
                            "GROUPING FOREIGN KEY(cid) REFERENCES c(cid)",
                            "order_date DATE");
        int i = createTable(SCHEMA, "i",
                            "iid INT PRIMARY KEY NOT NULL",
                            "oid INT NOT NULL",
                            "GROUPING FOREIGN KEY(oid) REFERENCES o(oid)",
                            "sku VARCHAR(10) NOT NULL");
        int a = createTable(SCHEMA, "a",
                            "aid INT PRIMARY KEY NOT NULL",
                            "cid INT NOT NULL",
                            "GROUPING FOREIGN KEY(cid) REFERENCES c(cid)",
                            "state CHAR(2)");
        writeRow(c, 1, "Smith");
        writeRow(o, 101, 1, "2012-12-12");
        writeRow(i, 10101, 101, "ABCD");
        writeRow(i, 10102, 101, "1234");
        writeRow(o, 102, 1, "2013-01-01");
        writeRow(a, 101, 1, "MA");
        writeRow(c, 2, "Jones");
        writeRow(a, 201, 2, "NY");
        writeRow(c, 3, "Adams");
        writeRow(o, 301, 3, "2010-04-01");
    }

    static final String C13 = "[\n" +
        "]";

    @Test
    public void writeJsonC13() throws IOException {
        ExternalDataService external = 
            serviceManager().getServiceByClass(ExternalDataService.class);
        StringWriter str = new StringWriter();
        PrintWriter pw = new PrintWriter(str);
        external.writeBranchAsJson(session(), pw, SCHEMA, "c", 
                                   Arrays.asList(Collections.singletonList("1"),
                                                 Collections.singletonList("3")),
                                   -1);
        assertEquals(C13, str.toString());
    }
}
