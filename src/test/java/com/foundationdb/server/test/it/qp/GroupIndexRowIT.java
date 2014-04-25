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

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexRowComposition;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.api.dml.scan.NewRow;
import org.junit.Test;

import java.util.Arrays;

import static com.foundationdb.qp.operator.API.ancestorLookup_Default;
import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static org.junit.Assert.assertEquals;

// Inspired by bug 987942

public class GroupIndexRowIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        user = createTable(
            "schema", "usr",
            "uid int not null",
            "primary key(uid)");
        memberInfo = createTable(
            "schema", "member_info",
            "profileID int not null",
            "lastLogin int",
            "primary key(profileId)",
            "grouping foreign key (profileID) references usr(uid)");
        entitlementUserGroup = createTable(
            "schema", "entitlement_user_group",
            "entUserGroupID int not null",
            "uid int",
            "primary key(entUserGroupID)",
            "grouping foreign key (uid) references member_info(profileID)");
        createLeftGroupIndex(new TableName("schema", "usr"), "gi", "entitlement_user_group.uid", "member_info.lastLogin");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new com.foundationdb.qp.rowtype.Schema(ais());
        userRowType = schema.tableRowType(table(user));
        memberInfoRowType = schema.tableRowType(table(memberInfo));
        entitlementUserGroupRowType = schema.tableRowType(table(entitlementUserGroup));
        groupIndexRowType = groupIndexType(Index.JoinType.LEFT, "entitlement_user_group.uid", "member_info.lastLogin");
        group = group(user);
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new NewRow[] {
            createNewRow(user, 1L),
            createNewRow(memberInfo, 1L, 20120424),
        };
        use(db);
    }

    @Test
    public void testIndexMetadata()
    {
        // Index row: e.uid, m.lastLogin, m.profileID, e.eugid
        // HKey for eug table: [U, e.uid, M, E, e.eugid]
        GroupIndex gi = (GroupIndex) groupIndexRowType.index();
        IndexRowComposition rowComposition = gi.indexRowComposition();
        assertEquals(4, rowComposition.getFieldPosition(0));
        assertEquals(2, rowComposition.getFieldPosition(1));
        assertEquals(1, rowComposition.getFieldPosition(2));
        assertEquals(3, rowComposition.getFieldPosition(3));
    }

    @Test
    public void testItemIndexToMissingCustomerAndOrder()
    {
        Operator indexScan = indexScan_Default(groupIndexRowType,
                                               IndexKeyRange.unbounded(groupIndexRowType),
                                               new API.Ordering(),
                                               memberInfoRowType);
        Operator plan =
            ancestorLookup_Default(
                indexScan,
                group,
                groupIndexRowType,
                Arrays.asList(userRowType, memberInfoRowType),
                API.InputPreservationOption.DISCARD_INPUT);
        Row[] expected = new Row[] {
            row(userRowType, 1L),
            row(memberInfoRowType, 1L, 20120424L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }


    private int user;
    private int memberInfo;
    private int entitlementUserGroup;
    private TableRowType userRowType;
    private TableRowType memberInfoRowType;
    private TableRowType entitlementUserGroupRowType;
    private IndexRowType groupIndexRowType;
    private Group group;
}
