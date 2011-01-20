package com.akiban.cserver.itests.bugs.bug704443;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.TableName;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.itests.ApiTestBase;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.*;

public final class DecimalsWithoutParamsIT extends ApiTestBase {
    @Test
    public void decimalHasNoParams() throws InvalidOperationException {
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

        for(Column column : getUserTable(t1Id).getColumns()) {
            assertEquals("param[0] for column " + column, Long.valueOf(10), column.getTypeParameter1());
            assertEquals("param[1] for column " + column, Long.valueOf(0), column.getTypeParameter2());
        }
        assertEquals("columns.size()", 3, getUserTable(t1Id).getColumns().size());
    }

    @Test
    public void decimalHasOneParams() throws InvalidOperationException {
        TableId t1Id = createTable("schema", "t1",
                "c1 DECIMAL(17) NOT NULL,PRIMARY KEY(c1)"
        );

        Set<TableName> t1TableNameSet = new HashSet<TableName>();
        t1TableNameSet.add(new TableName("schema", "t1"));

        assertEquals("user tables", t1TableNameSet, getUserTables().keySet());
        assertEquals("group tables size", 1, getGroupTables().size());

        writeRows(
                createNewRow(t1Id, "27")
        );
        expectFullRows( t1Id,
                createNewRow(t1Id, new BigDecimal("27"))
        );
        for(Column column : getUserTable(t1Id).getColumns()) {
            assertEquals("param[0] for column " + column, Long.valueOf(17), column.getTypeParameter1());
            assertEquals("param[1] for column " + column, Long.valueOf(0), column.getTypeParameter2());
        }
        assertEquals("columns.size()", 1, getUserTable(t1Id).getColumns().size());
    }
}
