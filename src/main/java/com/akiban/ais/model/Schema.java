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

import java.util.Map;
import java.util.TreeMap;

public class Schema {
    public static Schema create(AkibanInformationSchema ais, String schemaName) {
        ais.checkMutability();
        Schema schema = new Schema(schemaName);
        ais.addSchema(schema);
        return schema;
    }

    public String getName() {
        return name;
    }

    public Map<String, UserTable> getUserTables() {
        return userTables;
    }

    public UserTable getUserTable(String tableName) {
        return userTables.get(tableName);
    }

    void addUserTable(UserTable userTable) {
        userTables.put(userTable.getName().getTableName(), userTable);
    }

    void removeTable(String tableName) {
        userTables.remove(tableName);
    }

    public Map<String, Sequence> getSequences() {
        return sequences;
    }
    
    public Sequence getSequence (String sequenceName) {
        return sequences.get(sequenceName);
    }
    
    void addSequence (Sequence sequence) {
        sequences.put(sequence.getSequenceName().getTableName(), sequence);
    }
    
    void removeSequence (String sequenceName) {
        sequences.remove(sequenceName);
    }
    
    public Map<String, View> getViews() {
        return views;
    }

    public View getView(String viewName) {
        return views.get(viewName);
    }

    void addView(View view) {
        views.put(view.getName().getTableName(), view);
    }

    void removeView(String viewName) {
        views.remove(viewName);
    }

    Schema(String name) {
        this.name = name;
    }

    private final String name;
    private final Map<String, UserTable> userTables = new TreeMap<String, UserTable>();
    private final Map<String, Sequence> sequences = new TreeMap<String, Sequence>();
    private final Map<String, View> views = new TreeMap<String, View>();
}
