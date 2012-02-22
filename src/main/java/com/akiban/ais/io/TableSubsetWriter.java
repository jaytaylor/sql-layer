/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais.io;

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
