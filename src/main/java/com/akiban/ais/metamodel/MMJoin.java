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

import com.akiban.ais.gwtutils.SerializableEnumSet;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MMJoin implements Serializable, ModelNames {
    public static Join create(AkibanInformationSchema ais, Map<String, Object> map)
    {
        String parentSchemaName = (String) map.get(join_parentSchemaName);
        String parentTableName = (String) map.get(join_parentTableName);
        String childSchemaName = (String) map.get(join_childSchemaName);
        String childTableName = (String) map.get(join_childTableName);
        String joinName = (String) map.get(join_joinName);
        Integer joinWeight = (Integer) map.get(join_joinWeight);
        String groupName = (String) map.get(join_groupName);

        UserTable parent = ais.getUserTable(parentSchemaName, parentTableName);
        UserTable child = ais.getUserTable(childSchemaName, childTableName);
        Join join = Join.create(ais, joinName, parent, child);
        join.setWeight(joinWeight);
        if (groupName != null) {
            Group group = ais.getGroup(groupName);
            parent.setGroup(group);
            child.setGroup(group);
            join.setGroup(group);
        }
        int groupingUsageInt = (Integer) map.get(join_groupingUsage);
        join.setGroupingUsage(Join.GroupingUsage.values()[groupingUsageInt]);
        int sourceTypesInt = (Integer) map.get(join_sourceTypes);
        SerializableEnumSet<Join.SourceType> sourceTypes = new SerializableEnumSet<Join.SourceType>(Join.SourceType.class);
        sourceTypes.loadInt(sourceTypesInt);
        join.setSourceTypes(sourceTypes);
        return join;
    }

    public static Map<String, Object> map(Join join)
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(join_joinName, join.getName());
        UserTable parent = join.getParent();
        map.put(join_parentSchemaName, parent.getName().getSchemaName());
        map.put(join_parentTableName, parent.getName().getTableName());
        UserTable child = join.getChild();
        map.put(join_childSchemaName, child.getName().getSchemaName());
        map.put(join_childTableName, child.getName().getTableName());
        Group group = join.getGroup();
        map.put(join_groupName, group == null ? null : group.getName());
        map.put(join_joinWeight, join.getWeight());
        map.put(join_groupingUsage, join.getGroupingUsage().ordinal());
        map.put(join_sourceTypes, join.getSourceTypesInt());
        return map;
    }
}
