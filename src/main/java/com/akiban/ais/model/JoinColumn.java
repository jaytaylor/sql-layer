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

package com.akiban.ais.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class JoinColumn implements Serializable, ModelNames
{
    public static void create(AkibaInformationSchema ais, Map<String, Object> map)
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
        join.addJoinColumn(parentColumn, childColumn);
    }

    protected static JoinColumn create(Join join,
                                           Column parent,
                                           Column child)
    {
        return new JoinColumn(join, parent, child);
    }

    @SuppressWarnings("unused")
    private JoinColumn()
    {
        // GWT requires empty constructor
    }
    
    public Map<String, Object> map()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(joinColumn_parentSchemaName, parent.getTable().getName().getSchemaName());
        map.put(joinColumn_parentTableName, parent.getTable().getName().getTableName());
        map.put(joinColumn_parentColumnName, parent.getName());
        map.put(joinColumn_childSchemaName, child.getTable().getName().getSchemaName());
        map.put(joinColumn_childTableName, child.getTable().getName().getTableName());
        map.put(joinColumn_childColumnName, child.getName());
        map.put(joinColumn_joinName, join.getName());
        return map;
    }    

    @Override
    public String toString()
    {
        return "JoinColumn(" + child.getName() + " -> " + parent.getName() + ")";
    }

    public JoinColumn(Join join, Column parent, Column child)
    {
        this.join = join;
        this.parent = parent;
        this.child = child;
    }

    public Join getJoin()
    {
        return join;
    }

    public Column getParent()
    {
        return parent;
    }

    public Column getChild()
    {
        return child;
    }

    private Join join;
    private Column parent;
    private Column child;
}
