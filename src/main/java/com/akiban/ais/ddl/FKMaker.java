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

package com.akiban.ais.ddl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.staticgrouping.GroupingVisitorStub;

public class FKMaker extends GroupingVisitorStub<Map<TableName,String>> {
    private Map<TableName,String> ret = new HashMap<TableName, String>();

    @Override
    public void visitChild(TableName parentName, List<String> parentColumns, TableName childName, List<String> childColumns) {
        assert ! ret.containsKey(childName) : "already saw child " + childName + ": " + ret;
        StringBuilder scratch = new StringBuilder();

        String fkName = scratch.append("__akiban_fk_").append(ret.size()).toString();
        scratch.setLength(0);
        TableName.escape(parentName.getTableName(), scratch);
        String parent = scratch.toString();
        String pCols = columns(parentColumns, scratch);
        String cCols = columns(childColumns, scratch);

        String fk = String.format(",%nCONSTRAINT `%s` FOREIGN KEY `%s` (%s) REFERENCES %s (%s)",
                fkName, fkName, cCols, parent, pCols);
        ret.put(childName, fk);
    }

    private String columns(List<String> columns, StringBuilder scratch) {
        scratch.setLength(0);
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("can't have empty string");
        }
        for (String col : columns) {
            TableName.escape(col, scratch);
            scratch.append(',');
        }
        scratch.setLength(scratch.length() - 1);
        return scratch.toString();
    }

    @Override
    public Map<TableName,String> end() {
        return ret;
    }
}
