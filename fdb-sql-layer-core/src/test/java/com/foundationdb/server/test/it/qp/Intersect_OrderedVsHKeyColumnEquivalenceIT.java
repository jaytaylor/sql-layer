/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.test.it.qp;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import org.junit.Test;

import java.util.EnumSet;

import static com.foundationdb.qp.operator.API.*;

// Inspired by bug 1012892

public class Intersect_OrderedVsHKeyColumnEquivalenceIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
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
        createLeftGroupIndex(new TableName("schema", "item"), "item_value_state_gi",
                             "item.app_id",
                             "item.status",
                             "item_value_state.field_id",
                             "item_value_state.revision_to",
                             "item_value_state.value",
                             "item.created_on",
                             "item.item_id");
        createLeftGroupIndex(new TableName("schema", "item"), "no_value_item_value_state_gi",
                             "item.app_id",
                             "item.status",
                             "item_value_state.field_id",
                             "item_value_state.revision_to",
                             "item.created_on",
                             "item.item_id");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        itemRowType = schema.tableRowType(table(item));
        itemValueStateRowType = schema.tableRowType(table(itemValueState));
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
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new Row[] {
            row(item, 1L, 1L, 100L, 100L, 100L, 100L, 100L, 100L, 100L, 100L, 100L),
            row(itemValueState, 1L, 1L, 111L, 111L, 1L, 1L),
            row(itemValueState, 1L, 1L, 222L, 222L, 1L, 1L),
            row(itemValueState, 1L, 1L, 333L, 333L, 1L, 1L),
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
        Row[] expected = new Row[] {
            row(giNoValueItemValueState, 1L, 100L, 1L, 1L, 100L, 1L, 111L, 111L),
            row(giNoValueItemValueState, 1L, 100L, 1L, 1L, 100L, 1L, 222L, 222L),
            row(giNoValueItemValueState, 1L, 100L, 1L, 1L, 100L, 1L, 333L, 333L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
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
                    EnumSet.of(IntersectOption.OUTPUT_RIGHT, IntersectOption.SKIP_SCAN),
                    null,
                    true);
        return plan;
    }
    private boolean[] ascending(boolean... ascending)
    {
        return ascending;
    }
}
