package com.akiban.cserver.itests.ddl;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.itests.ApiTestBase;
import org.junit.Test;

import java.util.Collections;

import static junit.framework.Assert.*;

public final class BadCreatesIT extends ApiTestBase {
    @Test
    public void bug704443() throws InvalidOperationException {
        InvalidOperationException exception = null;
        try {
            ddl().createTable(session, "test", String.format("CREATE TABLE t1 (%s)",
                    "c1 DECIMAL NOT NULL, c2 DECIMAL NOT NULL, c3 DECIMAL NOT NULL, PRIMARY KEY(c1,c2,c3)"));
        } catch (InvalidOperationException e) {
            exception = e;
        }

        AkibaInformationSchema ais = ddl().getAIS(session);
        assertEquals("user tables", Collections.<TableName, UserTable>emptyMap(), ais.getUserTables());
        assertEquals("group tables", Collections.<TableName, GroupTable>emptyMap(), ais.getGroupTables());
        
        assertNotNull("expected exception", exception);
    }
}
