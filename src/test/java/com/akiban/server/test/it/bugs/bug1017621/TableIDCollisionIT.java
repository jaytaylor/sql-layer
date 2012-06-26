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

package com.akiban.server.test.it.bugs.bug1017621;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.service.config.Property;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertNotNull;

public class TableIDCollisionIT extends ITBase {
    private static UserTable simpleISTable() {
        final TableName FAKE_TABLE = new TableName(TableName.INFORMATION_SCHEMA, "fake_table");
        NewAISBuilder builder = AISBBasedBuilder.create();
        builder.userTable(FAKE_TABLE).colLong("id").pk("id");
        UserTable table = builder.ais().getUserTable(FAKE_TABLE);
        assertNotNull("Found table", table);
        return table;
    }

    @Override
    protected Collection<Property> startupConfigProperties() {
        // Something unique, since we are messing with IS tables
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void createRestartAndCreate() throws Exception {
        createTable("test", "t1", "id int");
        safeRestartTestServices();
        createTable("test", "t2", "id int");
        serviceManager().getSchemaManager().registerStoredInformationSchemaTable(simpleISTable(), 1);
        createTable("test", "t3", "id int");
    }
}
