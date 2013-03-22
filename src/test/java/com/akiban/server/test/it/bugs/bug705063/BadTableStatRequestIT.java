
package com.akiban.server.test.it.bugs.bug705063;

import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.RowDefNotFoundException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

public final class BadTableStatRequestIT extends ITBase {
    @Test(expected= RowDefNotFoundException.class)
    public void noTablesDefined() throws InvalidOperationException {
        dml().getTableStatistics(session(), -1, false);
    }

    @Test(expected= RowDefNotFoundException.class)
    public void wrongTableIdDefined() throws InvalidOperationException {
        int created = createATable();

        dml().getTableStatistics(session(), created + 31, false);
    }

    private int createATable() {
        try {
            return createTable("schema1", "test1", "id int not null primary key");
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
    }
}
