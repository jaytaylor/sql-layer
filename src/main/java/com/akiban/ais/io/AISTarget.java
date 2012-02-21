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

import java.util.Map;

import com.akiban.ais.metamodel.MMColumn;
import com.akiban.ais.metamodel.MMGroup;
import com.akiban.ais.metamodel.MMIndex;
import com.akiban.ais.metamodel.MMIndexColumn;
import com.akiban.ais.metamodel.MMJoin;
import com.akiban.ais.metamodel.MMJoinColumn;
import com.akiban.ais.metamodel.MMTable;
import com.akiban.ais.metamodel.MMType;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.Target;
import com.akiban.ais.model.Type;

public class AISTarget extends Target
{
    private final AkibanInformationSchema ais;
    private String lastTypename = null;
    private int expectedCount = 0;
    private int actualCount = 0;
    

    public AISTarget() {
        this.ais = new AkibanInformationSchema();
    }

    public AISTarget(AkibanInformationSchema ais) {
        this.ais = ais;
    }

    public AkibanInformationSchema getAIS() {
        return ais;
    }

    private void checkCounts() throws IllegalStateException {
        if (lastTypename != null && (expectedCount != actualCount)) {
            throw new IllegalStateException(String.format("Expected count does not match actual for '%s': %d vs %d",
                                                          lastTypename, expectedCount, actualCount));
        }
    }
    
    @Override
    public void deleteAll() {
    }

    @Override
    public void writeCount(int count) {
        checkCounts();
        expectedCount = count;
        actualCount = 0;
    }

    @Override
    public void close() {
        checkCounts();
    }

    @Override
    public void writeVersion(int modelVersion) {
    }

    @Override
    protected final void write(String typename, Map<String, Object> map) {
        ++actualCount;
        lastTypename = typename;

        if(typename == type) {
            MMType.create(ais, map);
        }
        else if(typename == group) {
            MMGroup.create(ais, map);
        }
        else if(typename == table) {
            MMTable.create(ais, map);
        }
        else if(typename == column) {
            Column userColumn = MMColumn.create(ais, map);
            // Hook the userColumn/groupColumn back up
            Table userTable = userColumn.getTable();
            String groupSchemaName = (String)map.get(column_groupSchemaName);
            if (userTable.isUserTable() && groupSchemaName != null) {
                String groupTableName = (String)map.get(column_groupTableName);
                String groupColumnName = (String)map.get(column_groupColumnName);
                Table groupTable = ais.getGroupTable(groupSchemaName, groupTableName);
                if (groupTable != null) {
                    Column groupColumn = groupTable.getColumn(groupColumnName);
                    userColumn.setGroupColumn(groupColumn);
                    groupColumn.setUserColumn(userColumn);
                }
            }
        }
        else if(typename == join) {
            MMJoin.create(ais, map);
        }
        else if(typename == joinColumn) {
            MMJoinColumn.create(ais, map);
        }
        else if(typename == index) {
            MMIndex.create(ais, map);
        }
        else if(typename == indexColumn) {
            MMIndexColumn.create(ais, map);
        }
        else {
            throw new IllegalArgumentException("Unexpected typename: " + typename);
        }
    }
}
