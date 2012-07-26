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

package com.akiban.ais.model;

import java.util.ArrayList;

public class AISTableNameChanger {
    public AISTableNameChanger(UserTable table)
    {
        this.table = table;
        this.newSchemaName = table.getName().getSchemaName();
        this.newTableName = table.getName().getTableName();
    }

    public AISTableNameChanger(UserTable table, TableName newName) {
        this(table, newName.getSchemaName(), newName.getTableName());
    }

    public AISTableNameChanger(UserTable table, String newSchemaName, String newTableName) {
        this.table = table;
        this.newSchemaName = newSchemaName;
        this.newTableName = newTableName;
    }

    public void setSchemaName(String newSchemaName) {
        this.newSchemaName = newSchemaName;
    }

    public void setNewTableName(String newTableName) {
        this.newTableName = newTableName;
    }

    public void doChange() {
        AkibanInformationSchema ais = table.getAIS();
        ais.removeTable(table.getName());
        TableName newName = new TableName(newSchemaName, newTableName);

        // Fix indexes because index names incorporate table name
        for (Index index : table.getIndexesIncludingInternal()) {
            index.setIndexName(new IndexName(newName, index.getIndexName().getName()));
        }
        // Join names too. Copy the joins because ais.getJoins() will be updated inside the loop
        NameGenerator nameGenerator = new DefaultNameGenerator();
        for (Join join : new ArrayList<Join>(ais.getJoins().values())) {
            if (join.getParent().getName().equals(table.getName())) {
                String newJoinName = nameGenerator.generateJoinName(newName,
                                                                    join.getChild().getName(),
                                                                    join.getJoinColumns());
                join.replaceName(newJoinName);
            } else if (join.getChild().getName().equals(table.getName())) {
                String newJoinName = nameGenerator.generateJoinName(join.getParent().getName(),
                                                                    newName,
                                                                    join.getJoinColumns());
                join.replaceName(newJoinName);
            }
        }
        // Rename the table and put back in AIS
        table.setTableName(newName);
        ais.addUserTable(table);
    }


    UserTable table;
    String newSchemaName;
    String newTableName;
}
