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

package com.akiban.server.test.it.loadableplan;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.loadableplan.LoadableOperator;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.types.AkType;

import java.sql.Types;
import java.util.Arrays;

import static com.akiban.qp.operator.API.groupScan_Default;
import static com.akiban.qp.operator.API.project_Default;

/** A loadable operator plan.
 * <code><pre>
psql "host=localhost port=15432 sslmode=disable user=user password=pass" test <<EOF
DROP TABLE test;
CREATE TABLE test(id INT PRIMARY KEY NOT NULL, value VARCHAR(10));
INSERT INTO test VALUES(1, 'aaa'), (2, 'bbb');
EOF
java -jar jmxterm-1.0-alpha-4-uber.jar -l localhost:8082 -n <<EOF
run -b com.akiban:type=PostgresServer loadPlan `pwd`/`ls target/akiban-server-*-SNAPSHOT-tests.jar` com.akiban.server.test.it.loadableplan.TestPlan
EOF
psql "host=localhost port=15432 sslmode=disable user=user password=pass" test <<EOF
call test('666')
EOF
 * </pre></code> 
 */
public class TestPlan extends LoadableOperator
{
    @Override
    public String name()
    {
        return "test";
    }

    @Override
    public Operator plan()
    {
        // select id, value, $1 from test
        Group group = ais().getGroup("test");
        UserTable testTable = ais().getUserTable("test", "test");
        RowType testRowType = schema().userTableRowType(testTable);
        return
            project_Default(
                groupScan_Default(group),
                testRowType,
                Arrays.asList(Expressions.field(testRowType, 0),
                              Expressions.field(testRowType, 1),
                              Expressions.variable(AkType.LONG, 0)));
    }

    @Override
    public int[] jdbcTypes()
    {
        return TYPES;
    }

    private static final int[] TYPES = new int[]{Types.INTEGER, Types.VARCHAR, Types.INTEGER};
}
