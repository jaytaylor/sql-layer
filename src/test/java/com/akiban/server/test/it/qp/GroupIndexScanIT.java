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

import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.*;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValueSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class GroupIndexScanIT extends ITBase {

    @Test
    public void scanAtLeastO () {
        Operator plan = API.indexScan_Default(giRowType, false, unboundedRange(giRowType), uTableRowType(o));
        compareResults(plan,
                array("01-01-2001", null),
                array("02-02-2002", "1111"),
                array("03-03-2003", null),
                array("03-03-2003", "3333")
        );
    }

    @Test
    public void scanAtLeastI () {
        Operator plan = API.indexScan_Default(giRowType, false, unboundedRange(giRowType), uTableRowType(i));
        compareResults(plan,
                array("02-02-2002", "1111"),
                array("03-03-2003", null),
                array("03-03-2003", "3333")
        );
    }

    @Test
    public void defaultDepth() {
        Operator explicit = API.indexScan_Default(giRowType, false, unboundedRange(giRowType), uTableRowType(i));
        Operator defaulted = API.indexScan_Default(giRowType, false, unboundedRange(giRowType));

        List<List<?>> explicitList = planToList(explicit);
        List<List<?>> defaultedList = planToList(defaulted);

        assertEqualLists("results", explicitList, defaultedList);
    }

    @Test(expected = IllegalArgumentException.class)
    public void scanAtLeastC () {
        API.indexScan_Default(giRowType, false, null, uTableRowType(c));
    }

    @Test(expected = IllegalArgumentException.class)
    public void scanAtLeastH () {
        API.indexScan_Default(giRowType, false, null, uTableRowType(h));
    }

    @Before
    public void setUp() {
        c = createTable(SCHEMA, "c", "cid int not null primary key, name varchar(32)");
        o = createTable(SCHEMA, "o", "oid int not null primary key, c_id int", "when varchar(32)", akibanFK("c_id", "c", "cid"));
        i = createTable(SCHEMA, "i", "iid int not null primary key, o_id int", "sku varchar(6)", akibanFK("o_id", "o", "oid"));
        h = createTable(SCHEMA, "h", "hid int not null primary key, i_id int", akibanFK("i_id", "i", "iid"));
        TableName groupName = getUserTable(c).getGroup().getName();
        GroupIndex gi = createGroupIndex(groupName, GI_NAME, "o.when, i.sku");

        schema = new Schema(ddl().getAIS(session()));
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        giRowType = schema.indexRowType(gi);

        writeRows(
                createNewRow(c, 1L, "One"),
                createNewRow(o, 10L, 1L, "01-01-2001"), // no children
                createNewRow(o, 11L, 1L, "02-02-2002"), // one child
                createNewRow(i, 100L, 11L, "1111"),
                createNewRow(o, 12L, 2L, "03-03-2003"), // orphaned, two children
                createNewRow(i, 101L, 12L, null),
                createNewRow(i, 102L, 12L, "3333")
        );
    }

    @After
    public void tearDown() {
        c = o = i = h = null;
        schema = null;
        giRowType = null;
        adapter = null;
    }

    private UserTableRowType uTableRowType(int tableId) {
        UserTable userTable = ddl().getAIS(session()).getUserTable(tableId);
        UserTableRowType rowType = schema.userTableRowType(userTable);
        if (rowType == null) {
            throw new NullPointerException(userTable.toString());
        }
        return rowType;
    }

    private void compareResults(Operator plan, Object[]... expectedResults) {
        assertEqualLists("rows scanned", nestedList(expectedResults), planToList(plan));
    }

    private List<List<?>> planToList(Operator plan) {
        List<List<?>> actualResults = new ArrayList<List<?>>();
        Cursor cursor =  API.cursor(plan, queryContext);
        cursor.open();
        try {
            ToObjectValueTarget target = new ToObjectValueTarget();
            for (Row row = cursor.next(); row != null; row = cursor.next()) {
                RowType rowType = row.rowType();
                int fields =
                    rowType instanceof IndexRowType
                    ? ((IndexRowType)rowType).index().getKeyColumns().size()
                    : rowType.nFields();
                Object[] rowArray = new Object[fields];
                for (int i=0; i < rowArray.length; ++i) {
                    Object fromRow;
                    if (Types3Switch.ON) {
                        fromRow = getObject(row.pvalue(i));
                    }
                    else {
                        ValueSource source = row.eval(i);
                        fromRow = target.convertFromSource(source);
                    }
                    rowArray[i] = fromRow;
                }
                actualResults.add(Arrays.asList(rowArray));
            }
        } finally {
            cursor.close();
        }
        return actualResults;
    }

    private List<List<?>> nestedList(Object[][] input) {
        List<List<?>> listList = new ArrayList<List<?>>();
        for (Object[] array : input) {
            listList.add(Arrays.asList(array));
        }
        return listList;
    }

    private IndexKeyRange unboundedRange(IndexRowType indexRowType)
    {
        return IndexKeyRange.unbounded(indexRowType);
    }

    private Integer c, o, i, h;
    private Schema schema;
    private PersistitAdapter adapter;
    private QueryContext queryContext;
    private IndexRowType giRowType;

    private final static String SCHEMA = "schema";
    private final static String GI_NAME = "when_sku";
}
