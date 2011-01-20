package com.akiban.cserver.itests.ddl;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.itests.ApiTestBase;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.*;

public final class BadCreatesIT extends ApiTestBase {
    @Test
    public void bug704443() throws InvalidOperationException {
        TableId t1Id = createTable("schema", "t1",
                "c1 DECIMAL NOT NULL, c2 DECIMAL NOT NULL, c3 DECIMAL NOT NULL, PRIMARY KEY(c1,c2,c3)"
        );

        Set<TableName> t1TableNameSet = new HashSet<TableName>();
        t1TableNameSet.add(new TableName("schema", "t1"));

        assertEquals("user tables", t1TableNameSet, getUserTables().keySet());
        assertEquals("group tables size", 1, getGroupTables().size());

        writeRows(
                createNewRow(t1Id, "10", "10", "10")
        );
        expectFullRows( t1Id,
                createNewRow(t1Id, new BigDecimal("10"), new BigDecimal("10"), new BigDecimal("10"))
        );
    }
}
