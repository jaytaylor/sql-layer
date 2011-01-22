package com.akiban.cserver.itests.bugs.bug705063;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.itests.ApiTestBase;
import org.junit.Test;

public final class BadTableStatRequestIT extends ApiTestBase {
    @Test(expected= NoSuchTableException.class)
    public void noTablesDefined_byID() throws InvalidOperationException {
        dml().getTableStatistics(session, TableId.of(1), false);
    }

    @Test(expected= NoSuchTableException.class)
    public void noTablesDefined_byName() throws InvalidOperationException {
        dml().getTableStatistics(session, TableId.of("schema1", "table1"), false);
    }

    @Test(expected= NoSuchTableException.class)
    public void wrongTableIdDefined_byID() throws InvalidOperationException {
        TableId created = createATable();
        final TableId wrong;
        try {
            wrong = TableId.of( 31 + ddl().resolveTableId(created).getTableId(null) );
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        dml().getTableStatistics(session, wrong, false);
    }

    @Test(expected= NoSuchTableException.class)
    public void wrongTableIdDefined_byName() throws InvalidOperationException {
        createATable();
        dml().getTableStatistics(session, TableId.of("schema1", "table27"), false);
    }

    private TableId createATable() {
        try {
            return createTable("schema1", "test1", "id int key");
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
    }
}
