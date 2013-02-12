package com.akiban.direct;

import com.akiban.ais.model.AkibanInformationSchema;
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
