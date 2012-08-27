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
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.EnumSet;

import static com.akiban.qp.operator.API.*;

// Inspired by bug 1012892

public class Intersect_OrderedVsHKeyColumnEquivalenceIT extends OperatorITBase
{
    @Before
    public void before()
    {
        item = createTable(
            "schema", "item",
            "item_id int not null", // FIXED
            "app_id int",           // FIXED
            "space_id int",
            "external_id int",
            "status int",
            "current_revision int",
            "title int",
            "created_by_type int",
            "created_by_id int",
            "created_on int",
            "created_via int",
            "primary key (item_id)");
        itemValueState = createTable(
            "schema", "item_value_state",
            "item_id int not null",   // FIXED (due to join)
            "field_id int not null",  // FIXED
            "delta int not null",
            "revision_from int not null",
            "revision_to int",        // FIXED
            "value int",              // FIXED
            "primary key(item_id, field_id, delta, revision_from)",
            "grouping foreign key (item_id) references item(item_id)");
        createGroupIndex("item", "item_value_state_gi",
                         "item.app_id, " +
                         "item.status, " +
                         "item_value_state.field_id, " +
                         "item_value_state.revision_to, " +
                         "item_value_state.value, " +
                         "item.created_on, " +
                         "item.item_id");
        createGroupIndex("item", "no_value_item_value_state_gi",
                         "item.app_id," +
                         "item.status," +
                         "item_value_state.field_id," +
                         "item_value_state.revision_to," +
                         "item.created_on," +
                         "item.item_id");
        schema = new Schema(rowDefCache().ais());
        itemRowType = schema.userTableRowType(userTable(item));
        itemValueStateRowType = schema.userTableRowType(userTable(itemValueState));
        giItemValueState =
            groupIndexType(Index.JoinType.LEFT,
                           "item.app_id",
                           "item.status",
                           "item_value_state.field_id",
                           "item_value_state.revision_to",
                           "item_value_state.value",
                           "item.created_on",
                           "item.item_id");
        giNoValueItemValueState =
            groupIndexType(Index.JoinType.LEFT,
                           "item.app_id",
                           "item.status",
                           "item_value_state.field_id",
                           "item_value_state.revision_to",
                           "item.created_on",
                           "item.item_id");
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        db = new NewRow[] {
            createNewRow(item, 1L, 1L, 100L, 100L, 100L, 100L, 100L, 100L, 100L, 100L, 100L),
            createNewRow(itemValueState, 1L, 1L, 111L, 111L, 1L, 1L),
            createNewRow(itemValueState, 1L, 1L, 222L, 222L, 1L, 1L),
            createNewRow(itemValueState, 1L, 1L, 333L, 333L, 1L, 1L),
        };
        use(db);
    }
    
    private int item;
    private int itemValueState;
    private RowType itemRowType;
    private RowType itemValueStateRowType;
    private IndexRowType giItemValueState;
    private IndexRowType giNoValueItemValueState;

    @Test
    public void test()
    {
        Operator plan = intersectPlan();
        RowBase[] expected = new RowBase[] {
            row(giNoValueItemValueState, 1L, 100L, 1L, 1L, 100L, 1L, 111L, 111L),
            row(giNoValueItemValueState, 1L, 100L, 1L, 1L, 100L, 1L, 222L, 222L),
            row(giNoValueItemValueState, 1L, 100L, 1L, 1L, 100L, 1L, 333L, 333L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    private Operator intersectPlan()
    {
        Operator plan =
            intersect_Ordered(
                indexScan_Default(giItemValueState),
                indexScan_Default(giNoValueItemValueState),
                giItemValueState,
                giNoValueItemValueState,
                4,
                4,
                ascending(true, true, true, true),
                JoinType.INNER_JOIN,
                EnumSet.of(IntersectOption.OUTPUT_RIGHT, IntersectOption.SKIP_SCAN));
        return plan;
    }
    private boolean[] ascending(boolean... ascending)
    {
        return ascending;
    }
}
