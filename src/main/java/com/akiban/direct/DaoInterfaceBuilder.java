package com.akiban.direct;

import com.akiban.ais.model.AkibanInformationSchema;
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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;

public class DaoInterfaceBuilder {
    
    private final static String[] NONE = new String[0];
    private final static String PACKAGE = "com.akiban.direct.entity";

    public void generateSchema(ClassBuilder helper, AkibanInformationSchema ais, String schema) {
        helper.preamble(new String[] { "java.util.Date", "java.util.List" });
        String schemaAsClassName = PACKAGE + "." + ClassBuilder.asJavaName(schema, true);
        helper.startClass(schemaAsClassName);
        for (final UserTable table : ais.getSchema(schema).getUserTables().values()) {
            generateInterface(helper, table, schemaAsClassName);
        }
        helper.end();
    }

    private void generateInterface(ClassBuilder helper, UserTable table, String schemaAsClassName) {
        table.getName().getTableName();
        String typeName = schemaAsClassName + "$" + ClassBuilder.asJavaName(table.getName().getTableName(), true);
        helper.startClass(typeName);
        /*
         * Add a property per column
         */
        for (final Column column : table.getColumns()) {
            Class<?> javaClass = column.getType().akType().javaClass();
            helper.addProperty(column.getName(), javaClass.getName(), null, null, null);
        }

        /*
         * Add an accessor for the parent row if there is one
         */
        Join parentJoin = table.getParentJoin();
        if (parentJoin != null) {
            String parentTypeName = parentJoin.getParent().getName().getTableName();
            helper.addMethod("get" + ClassBuilder.asJavaName(parentTypeName, true),
                    ClassBuilder.asJavaName(parentTypeName, true), NONE, null, null);
        }

        /*
         * Add an accessor for each child table.
         */
        for (final Join join : table.getChildJoins()) {
            String childTypeName = join.getChild().getName().getTableName();
            helper.addMethod("get" + ClassBuilder.asJavaName(childTypeName, true),
                    "List<" + ClassBuilder.asJavaName(childTypeName, true) + ">", NONE, null, null);
        }
        /*
         * Add boilerplate methods
         */
        helper.addMethod("copy", typeName, NONE, null, null);
        helper.addMethod("save", "void", NONE, null, null);

        helper.end();
    }


}
