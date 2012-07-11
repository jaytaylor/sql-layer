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

import java.util.Map;

import com.akiban.ais.metamodel.MMColumn;
import com.akiban.ais.metamodel.MMGroup;
import com.akiban.ais.metamodel.MMIndex;
import com.akiban.ais.metamodel.MMIndexColumn;
import com.akiban.ais.metamodel.MMJoin;
import com.akiban.ais.metamodel.MMJoinColumn;
import com.akiban.ais.metamodel.MMTable;
import com.akiban.ais.metamodel.MMType;
import com.akiban.ais.metamodel.Target;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Table;


/**
 * @deprecated Does not preserve all AIS objects, only those that exist in MetaModel
 */
@Deprecated
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
