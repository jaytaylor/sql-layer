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

package com.akiban.ais.metamodel.io;

import com.akiban.ais.metamodel.Target;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.UserTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

public abstract class TableSubsetWriter extends Writer
{
    public TableSubsetWriter(Target target) {
        super(target);
    }

    public abstract boolean shouldSaveTable(Table table);
    
    protected Collection<Group> getGroups(AkibanInformationSchema ais) {
        Collection<Group> groups = new HashSet<Group>();
        for (UserTable table : ais.getUserTables().values()) {
            if (shouldSaveTable(table)) {
                groups.add(table.getGroup());
            }
        }
        return groups;
    }

    protected Collection<GroupTable> getGroupTables(AkibanInformationSchema ais) {
        Collection<GroupTable> groupTables = new ArrayList<GroupTable>();
        for (GroupTable table : ais.getGroupTables().values()) {
            if (shouldSaveTable(table)) {
                groupTables.add(table);
            }
        }
        return groupTables;
    }

    protected Collection<UserTable> getUserTables(AkibanInformationSchema ais) {
        Collection<UserTable> userTables = new ArrayList<UserTable>();
        for (UserTable table : ais.getUserTables().values()) {
            if (shouldSaveTable(table)) {
                userTables.add(table);
            }
        }
        return userTables;
    }

    protected Collection<Join> getJoins(AkibanInformationSchema ais) {
        Collection<Join> joins = new ArrayList<Join>();
        for (Join join : ais.getJoins().values()) {
            // TODO: Should probably be || but join constructor doesn't handle missing table
            if (shouldSaveTable(join.getParent()) && shouldSaveTable(join.getChild())) {
                joins.add(join);
            }
        }
        return joins;
    }
}
