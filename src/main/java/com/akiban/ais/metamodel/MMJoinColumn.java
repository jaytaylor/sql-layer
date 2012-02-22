/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

package com.akiban.ais.metamodel;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.UserTable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MMJoinColumn implements Serializable, ModelNames {
    public static JoinColumn create(AkibanInformationSchema ais, Map<String, Object> map)
    {
        String parentSchemaName = (String) map.get(joinColumn_parentSchemaName);
        String parentTableName = (String) map.get(joinColumn_parentTableName);
        String parentColumnName = (String) map.get(joinColumn_parentColumnName);
        String childSchemaName = (String) map.get(joinColumn_childSchemaName);
        String childTableName = (String) map.get(joinColumn_childTableName);
        String childColumnName = (String) map.get(joinColumn_childColumnName);
        String joinName = (String) map.get(joinColumn_joinName);
        Join join = ais.getJoin(joinName);
        UserTable parentTable = ais.getUserTable(parentSchemaName, parentTableName);
        UserTable childTable = ais.getUserTable(childSchemaName, childTableName);
        assert join.getParent() == parentTable;
        assert join.getChild() == childTable;
        Column parentColumn = parentTable.getColumn(parentColumnName);
        Column childColumn = childTable.getColumn(childColumnName);
        return JoinColumn.create(join, parentColumn, childColumn);
    }

    public static Map<String, Object> map(JoinColumn joinColumn)
    {
        Map<String, Object> map = new HashMap<String, Object>();
        Column parent = joinColumn.getParent();
        map.put(joinColumn_parentSchemaName, parent.getTable().getName().getSchemaName());
        map.put(joinColumn_parentTableName, parent.getTable().getName().getTableName());
        map.put(joinColumn_parentColumnName, parent.getName());
        Column child = joinColumn.getChild();
        map.put(joinColumn_childSchemaName, child.getTable().getName().getSchemaName());
        map.put(joinColumn_childTableName, child.getTable().getName().getTableName());
        map.put(joinColumn_childColumnName, child.getName());
        map.put(joinColumn_joinName, joinColumn.getJoin().getName());
        return map;
    }

}
