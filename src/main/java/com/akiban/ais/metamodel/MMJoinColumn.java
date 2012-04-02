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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.UserTable;

import java.util.HashMap;
import java.util.Map;

public class MMJoinColumn implements ModelNames {
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
