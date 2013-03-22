
package com.akiban.server.test.it.bugs.bug1081621;

import com.akiban.ais.model.TableName;
import com.akiban.ais.util.TableChangeValidator;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

public class AlterAffectingPKLosesTreeIT extends ITBase {
    private final static String SCHEMA = "test";
    private final static TableName P_NAME = new TableName(SCHEMA, "p");
    private final static TableName C_NAME = new TableName(SCHEMA, "c");

    private void createTables() {
        createTable(P_NAME, "id int not null primary key, x int");
        createTable(C_NAME, "id int not null primary key, pid int, grouping foreign key(pid) references p(id)");
    }

    @Test
    public void test() throws Exception {
        createTables();

        runAlter(TableChangeValidator.ChangeLevel.GROUP, SCHEMA, "ALTER TABLE p DROP COLUMN id");
        runAlter(TableChangeValidator.ChangeLevel.GROUP, SCHEMA, "ALTER TABLE c DROP COLUMN id");

        ddl().dropTable(session(), P_NAME);
        ddl().dropTable(session(), C_NAME);

        safeRestartTestServices();

        createTables();
    }
}
