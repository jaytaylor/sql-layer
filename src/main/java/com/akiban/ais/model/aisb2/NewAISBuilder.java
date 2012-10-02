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

package com.akiban.ais.model.aisb2;

import com.akiban.ais.model.TableName;

public interface NewAISBuilder extends NewAISProvider {
    /**
     * Sets the default schema
     * @param schema the new default schema name; like SQL's {@code USING}.
     * @return {@code this}
     */
    NewAISBuilder defaultSchema(String schema);

    /**
     * Starts creating a new table using the default schema.
     * @param table the table's name
     * @return the new table's builder
     */
    NewUserTableBuilder userTable(String table);

    /**
     * Starts creating a new table using the given schema
     * @param schema the new table's schema
     * @param table the new table's table name
     * @return the new table's builder
     */
    NewUserTableBuilder userTable(String schema, String table);

    NewUserTableBuilder userTable(TableName tableName);
   
    /**
     * create a new sequence
     */
    NewAISBuilder sequence (String name);
    NewAISBuilder sequence (String name, long start, long increment, boolean isCycle);

    /**
     * create a new view 
     * @param view
     * @return
     */
    NewViewBuilder view(String view);

    NewViewBuilder view(String schema, String view);

    NewViewBuilder view(TableName viewName);

    /**
     * create a new procedure 
     * @param procedure
     * @return
     */
    NewRoutineBuilder procedure(String procedure);

    NewRoutineBuilder procedure(String schema, String procedure);

    NewRoutineBuilder procedure(TableName procedureName);
}
