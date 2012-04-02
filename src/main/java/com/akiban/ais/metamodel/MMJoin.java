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

package com.akiban.ais.metamodel;

import com.akiban.ais.gwtutils.SerializableEnumSet;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;

import java.util.HashMap;
import java.util.Map;

public class MMJoin implements ModelNames {
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
