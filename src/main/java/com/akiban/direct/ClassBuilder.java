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
package com.akiban.direct;

import java.util.Map;
import java.util.TreeMap;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;

public abstract class ClassBuilder {
    final static String PACKAGE = "com.akiban.direct.entity";
    final static String[] NONE = {};
    final static String[] IMPORTS = { java.util.Date.class.getName(), com.akiban.direct.DirectList.class.getName() };
    final static String[] UNSUPPORTED = { "// TODO", "throw new UnsupportedOperationException()" };

    protected final String packageName;
    protected String schema;

    public abstract void startClass(String name, boolean isInterface, String extendsClass, String[] implementsClasses,
            String[] imports) throws CannotCompileException, NotFoundException;

    public abstract void addMethod(String name, String returnTuype, String[] argumentTypes, String[] argumentNames,
            String[] body);

    protected ClassBuilder(String packageName, String schema) {
        this.packageName = packageName;
        this.schema = schema;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.akiban.direct.ClassBuilder#property(java.lang.String,
     * java.lang.String)
     */
    public void addProperty(final String name, final String type, final String argName, final String[] getBody,
            final String[] setBody) {
        String caseConverted = asJavaName(name, true);
        boolean b = "boolean".equals(type);
        addMethod((b ? "is" : "get") + caseConverted, type, new String[0], null, getBody);
        addMethod("set" + caseConverted, "void", new String[] { type },
                new String[] { argName == null ? "v" : argName }, setBody);
    }

    public abstract void end();

    /*
     * Complete demo hack for now. Remove file s from table name (because we
     * want a singular name in the generated code). Make other assumptions about
     * uniqueness, etc.
     */
    public static String asJavaName(final String name, final boolean toUpper) {
        StringBuilder sb = new StringBuilder(name);
        if (sb.length() > 1 && name.charAt(sb.length() - 1) == 's') {
            if (sb.length() > 2 && name.charAt(sb.length() - 2) == 'e') {
                sb.setLength(sb.length() - 2);
            } else {
                sb.setLength(sb.length() - 1);
            }
        }
        boolean isUpper = Character.isUpperCase(sb.charAt(0));
        if (toUpper && !isUpper) {
            sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));
        }
        if (!toUpper && isUpper) {
            sb.setCharAt(0, Character.toLowerCase(sb.charAt(0)));
        }
        return sb.toString();
    }

    public static String schemaClassName(String schema) {
        return PACKAGE + "." + asJavaName(schema, true);
    }

    public static Map<Integer, CtClass> compileGeneratedInterfacesAndClasses(final AkibanInformationSchema ais,
            final String schema) throws CannotCompileException, NotFoundException {
        /*
         * Precompile the interfaces
         */
        ClassPool pool = new ClassPool(true);
        ClassObjectWriter helper = new ClassObjectWriter(pool, PACKAGE, schema);
        String scn = schemaClassName(schema);
        helper.startClass(scn, true, null, null, IMPORTS);
        for (final UserTable table : ais.getSchema(schema).getUserTables().values()) {
            helper.generateInterfaceClass(table, scn);
        }
        helper.end();

        /*
         * Precompile the implementation classes
         */

        Map<Integer, CtClass> implClassMap = new TreeMap<Integer, CtClass>();
        for (final UserTable table : ais.getSchema(schema).getUserTables().values()) {
            helper.generateImplementationClass(table, schema);
            implClassMap.put(table.getTableId(), helper.getCurrentClasse());
        }
        return implClassMap;
    }

    public void writeGeneratedInterfaces(final AkibanInformationSchema ais, final String schema)
            throws CannotCompileException, NotFoundException {
        String scn = schemaClassName(schema);
        startClass(scn, true, null, null, IMPORTS);
        for (final UserTable table : ais.getSchema(schema).getUserTables().values()) {
            generateInterfaceClass(table, scn);
        }
        end();
    }

    public void writeGeneratedClass(final AkibanInformationSchema ais, final String schema, final String name)
            throws CannotCompileException, NotFoundException {
        UserTable table = ais.getUserTable(new TableName(schema, name));
        generateImplementationClass(table, schema);
    }

    public void generateInterfaceClass(UserTable table, String schemaAsClassName) throws CannotCompileException,
            NotFoundException {
        table.getName().getTableName();
        String typeName = schemaAsClassName + "$" + asJavaName(table.getName().getTableName(), true);
        startClass(typeName, true, null, null, null);
        /*
         * Add a property per column
         */
        for (final Column column : table.getColumns()) {
            Class<?> javaClass = column.getType().akType().javaClass();
            addProperty(column.getName(), javaClass.getName(), null, null, null);
        }

        /*
         * Add an accessor for the parent row if there is one
         */
        Join parentJoin = table.getParentJoin();
        if (parentJoin != null) {
            String parentTypeName = parentJoin.getParent().getName().getTableName();
            addMethod("get" + asJavaName(parentTypeName, true), asJavaName(parentTypeName, true), NONE, null, null);
        }

        /*
         * Add an accessor for each child table.
         */
        for (final Join join : table.getChildJoins()) {
            String childTypeName = join.getChild().getName().getTableName();
            addMethod("get" + asJavaName(childTypeName, true), "com.akiban.direct.DirectList<" + schemaAsClassName
                    + "$" + asJavaName(childTypeName, true) + ">", NONE, null, null);
        }
        /*
         * Add boilerplate methods
         */
        addMethod("copy", typeName, NONE, null, null);
        addMethod("save", "void", NONE, null, null);

        end();
    }

    void generateImplementationClass(UserTable table, String schema) throws CannotCompileException, NotFoundException {
        table.getName().getTableName();
        String scn = schemaClassName(schema);
        String typeName = scn + "$" + asJavaName(table.getName().getTableName(), true);
        String className = packageName + "._" + asJavaName(schema, true) + "_"
                + asJavaName(table.getName().getTableName(), true);
        startClass(className, false, "com.akiban.direct.AbstractDirectObject", new String[] { typeName }, IMPORTS);

        /*
         * Add a property per column
         */
        for (final Column column : table.getColumns()) {
            Class<?> javaClass = column.getType().akType().javaClass();
            String[] getBody = new String[] { "return __get" + column.getType().akType() + "(" + column.getPosition()
                    + ")" };
            addProperty(column.getName(), javaClass.getName(), null, getBody, UNSUPPORTED);
        }

        /*
         * Add an accessor for the parent row if there is one
         */
        Join parentJoin = table.getParentJoin();
        if (parentJoin != null) {
            String parentTypeName = parentJoin.getParent().getName().getTableName();
            addMethod("get" + asJavaName(parentTypeName, true), asJavaName(parentTypeName, true), NONE, null,
                    UNSUPPORTED);
        }

        /*
         * Add an accessor for each child table.
         */
        for (final Join join : table.getChildJoins()) {
            String childTypeName = join.getChild().getName().getTableName();
            addMethod("get" + asJavaName(childTypeName, true), "com.akiban.direct.DirectList<" + scn + "."
                    + asJavaName(childTypeName, true) + ">", NONE, null, UNSUPPORTED);
        }
        /*
         * Add boilerplate methods
         */
        addMethod("copy", typeName, NONE, null, UNSUPPORTED);

        end();
    }

}