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

import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexRowComposition;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static com.akiban.qp.operator.API.ancestorLookup_Default;
import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;
import static org.junit.Assert.assertEquals;

// Inspired by bug 987942

public class GroupIndexRowIT extends OperatorITBase
{
    @Before
    public void before()
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
        createGroupIndex("usr", "gi", "entitlement_user_group.uid,member_info.lastLogin", Index.JoinType.LEFT);
        schema = new com.akiban.qp.rowtype.Schema(ais());
        userRowType = schema.userTableRowType(userTable(user));
        memberInfoRowType = schema.userTableRowType(userTable(memberInfo));
        entitlementUserGroupRowType = schema.userTableRowType(userTable(entitlementUserGroup));
        groupIndexRowType = groupIndexType(Index.JoinType.LEFT, "entitlement_user_group.uid", "member_info.lastLogin");
        group = group(user);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
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
        RowBase[] expected = new RowBase[] {
            row(userRowType, 1L),
            row(memberInfoRowType, 1L, 20120424L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }


    private int user;
    private int memberInfo;
    private int entitlementUserGroup;
    private UserTableRowType userRowType;
    private UserTableRowType memberInfoRowType;
    private UserTableRowType entitlementUserGroupRowType;
    private IndexRowType groupIndexRowType;
    private Group group;
}
