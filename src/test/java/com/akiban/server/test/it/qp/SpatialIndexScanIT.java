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

package com.akiban.server.test.it.qp;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableIndex;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.types.ValueSource;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;
import static junit.framework.Assert.assertEquals;

@Ignore
public class SpatialIndexScanIT extends OperatorITBase
{
    @Before
    public void before()
    {
        point = createTable(
            "schema", "point",
            "id int not null",
            "x int",
            "y int",
            "primary key(id)");
        TableIndex xyIndex = createIndex("schema", "point", "xy", "x", "y");
        xyIndex.spatialIndexDimensions(LO, HI);
        schema = new Schema(rowDefCache().ais());
        pointRowType = schema.userTableRowType(userTable(point));
        xyIndexRowType = indexType(point, "x", "y");
        group = groupTable(point);
        db = new NewRow[]{
        };
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        use(db);
    }

    @Test
    public void test()
    {
        long id = 0;
        for (long x = 100; x <= 300; x += 100) {
            for (long y = 100; y <= 300; y += 100) {
                dml().writeRow(session(), createNewRow(point, id, x, y));
                id++;
            }
        }
        Operator plan = indexScan_Default(xyIndexRowType);
        dump(plan);
    }

    private static final long[] LO = {0L, 0L};
    private static final long[] HI = {999L, 999L};

    private int point;
    private UserTableRowType pointRowType;
    private IndexRowType xyIndexRowType;
    private GroupTable group;
}
