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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.types.AkType;
import com.sun.jersey.core.impl.provider.entity.Inflector;

public abstract class ClassBuilder {
    final static String PACKAGE = "com.akiban.direct.entity";
    final static String[] NONE = {};
    final static String[] IMPORTS = { java.util.Date.class.getName(), com.akiban.direct.DirectIterable.class.getName() };
    final static String[] UNSUPPORTED = { "// TODO", "throw new UnsupportedOperationException()" };

    private final static Inflector INFLECTOR = Inflector.getInstance();

    protected final String packageName;
    protected String schema;

    public abstract void startClass(String name, boolean isInterface, String extendsClass, String[] implementsClasses,
            String[] imports) throws CannotCompileException, NotFoundException;

    public abstract void addMethod(String name, String returnType, String[] argumentTypes, String[] argumentNames,
            String[] body);

    public abstract void addConstructor(String[] argumentTypes, String[] argumentNames, String[] body);

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
    public String addProperty(final String name, final String type, final String argName, final String[] getBody,
            final String[] setBody) {
        String caseConverted = asJavaName(name, true);
        boolean b = "boolean".equals(type);
        String getName = (b ? "is" : "get") + caseConverted;
        addMethod((b ? "is" : "get") + caseConverted, type, new String[0], null, getBody);
        addMethod("set" + caseConverted, "void", new String[] { type },
                new String[] { argName == null ? "v" : argName }, setBody);
        return getName + "()";
    }

    public abstract void end();

    public static String asJavaName(final String name, final boolean toUpper) {
        return INFLECTOR.camelize(INFLECTOR.singularize(name), !toUpper);
    }
    
    public static String asJavaCollectionName(final String name, final boolean toUpper) {
        return INFLECTOR.camelize(name, !toUpper);
    }

    public static String schemaClassName(String schema) {
        return PACKAGE + "." + INFLECTOR.classify(schema);
    }

    public static Map<Integer, CtClass> compileGeneratedInterfacesAndClasses(final AkibanInformationSchema ais,
            final String schema) throws CannotCompileException, NotFoundException {
        /*
         * Precompile the interfaces
         */
        ClassPool pool = new ClassPool(true);
        ClassObjectWriter helper = new ClassObjectWriter(pool, PACKAGE, schema);
        String scn = schemaClassName(schema);
        Map<Integer, CtClass> implClassMap = new TreeMap<Integer, CtClass>();

        helper.startClass(scn, true, null, null, IMPORTS);
        for (final UserTable table : ais.getSchema(schema).getUserTables().values()) {
            if (table.isRoot()) {
                helper.addExtentAccessor(table, scn, true);
            }
            helper.generateInterfaceClass(table, scn);
            implClassMap.put(-table.getTableId(), helper.getCurrentClass());
        }
        helper.end();
        implClassMap.put(Integer.MIN_VALUE, helper.getCurrentClass());

        /*
         * Precompile the implementation classes
         */

        for (final UserTable table : ais.getSchema(schema).getUserTables().values()) {
            helper.generateImplementationClass(table, schema);
            implClassMap.put(table.getTableId(), helper.getCurrentClass());
        }
        /*
         * Precompile the extent class
         */
        helper.startExtentClass(schema, scn);
        for (final UserTable table : ais.getSchema(schema).getUserTables().values()) {
            if (table.isRoot()) {
                helper.addExtentAccessor(table, scn, false);
            }
        }
        helper.end();
        implClassMap.put(Integer.MAX_VALUE, helper.getCurrentClass());

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

    public void generateInterfaceClass(UserTable table, String scn) throws CannotCompileException, NotFoundException {
        table.getName().getTableName();
        String typeName = scn + "$" + asJavaName(table.getName().getTableName(), true);
        startClass(typeName, true, null, null, null);
        addMethods(table, scn, typeName, typeName, true);
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
        addConstructor(NONE, NONE, NONE);
        addMethods(table, scn, typeName, className, false);
        end();
    }

    void startExtentClass(String schema, final String scn) throws CannotCompileException, NotFoundException {
        String className = packageName + "._" + asJavaName(schema, true);
        startClass(className, false, "com.akiban.direct.AbstractDirectObject", new String[] { scn }, IMPORTS);
    }

    void addExtentAccessor(UserTable table, String scn, boolean iface) {
        String tableName = table.getName().getTableName();
        String className = scn + "$" + asJavaName(tableName, true);

        String[] body = null;
        if (!iface) {
            StringBuilder sb = new StringBuilder(buildDirectIterableExpr(className, tableName));
            body = new String[] { "return " + sb.toString() };
        }
        addMethod("get" + asJavaCollectionName(tableName, true), "com.akiban.direct.DirectIterable<" + className + ">",
                NONE, null, body);

    }

    private void addMethods(UserTable table, String scn, String typeName, String className, boolean iface) {
        /*
         * Add a property per column
         */
        Map<String, String> getterMethods = new HashMap<String, String>();
        for (final Column column : table.getColumns()) {
            Class<?> javaClass = javaClass(column);
            String[] getBody = new String[] { "return __get" + column.getType().akType() + "(" + column.getPosition()
                    + ")" };
            String expr = addProperty(column.getName(), javaClass.getName(), null, iface ? null : getBody, iface ? null
                    : UNSUPPORTED);
            getterMethods.put(column.getName(), expr);
        }

        /*
         * Add an accessor for the parent row if there is one
         */
        Join parentJoin = table.getParentJoin();
        if (parentJoin != null) {
            String parentTableName = parentJoin.getParent().getName().getTableName();
            String parentClassName = scn + "$" + asJavaName(parentTableName, true);
            String[] body = null;
            if (!iface) {
                StringBuilder sb = new StringBuilder(buildDirectIterableExpr(parentClassName, parentTableName));
                for (final JoinColumn jc : parentJoin.getJoinColumns()) {
                    sb.append(String.format(".where(\"%s\", %s)", jc.getParent().getName(),
                            literal(getterMethods, jc.getParent())));
                }
                body = new String[] { "return " + sb.toString() + ".single()" };
            }
            addMethod("get" + asJavaName(parentTableName, true), parentClassName, NONE, null, body);
        }

        /*
         * Add an accessor for each child table.
         */
        for (final Join join : table.getChildJoins()) {

            List<Column> primaryKeyColumns = join.getChild().getPrimaryKey().getColumns();
            /*
             * Remove any PK columns also found in the parent.
             */
            for (final Iterator<Column> iter = primaryKeyColumns.iterator(); iter.hasNext();) {
                Column childColumn = iter.next();
                if (join.getMatchingParent(childColumn) != null) {
                    iter.remove();
                }
            }
            String childTableName = join.getChild().getName().getTableName();
            String childClassName = scn + "$" + asJavaName(childTableName, true);
            if (primaryKeyColumns.size() <= 1) {
                /*
                 * Add a child accessor by primary key value
                 */
                String[] types = new String[primaryKeyColumns.size()];
                for (int i = 0; i < types.length; i++) {
                    types[i] = javaClass(primaryKeyColumns.get(i)).getName();
                }
                addMethod("get" + asJavaName(childTableName, true), childClassName, types, null, iface ? null
                        : UNSUPPORTED);
            }
            if (!primaryKeyColumns.isEmpty()) {
                String[] body = null;
                if (!iface) {
                    StringBuilder sb = new StringBuilder(buildDirectIterableExpr(childClassName, childTableName));
                    for (final JoinColumn jc : join.getJoinColumns()) {
                        sb.append(String.format(".where(\"%s\", %s)", jc.getChild().getName(),
                                literal(getterMethods, jc.getParent())));
                    }
                    body = new String[] { "return " + sb.toString() };
                }
                addMethod("get" + asJavaCollectionName(childTableName, true), "com.akiban.direct.DirectIterable<"
                        + childClassName + ">", NONE, null, body);
            }
        }
        /*
         * Add boilerplate methods
         */
        addMethod("copy", typeName, NONE, null, iface ? null : UNSUPPORTED);
    }

    private Class<?> javaClass(final Column column) {
        AkType type = column.getType().akType();

        switch (type) {
        case DATE:
            return java.sql.Date.class;
        case DATETIME:
            return java.sql.Date.class;
        case DECIMAL:
            return java.math.BigDecimal.class;
        case DOUBLE:
            return Double.TYPE;
        case FLOAT:
            return Float.TYPE;
        case INT:
            return Integer.TYPE;
        case LONG:
            return Long.TYPE;
        case VARCHAR:
            return java.lang.String.class;
        case TEXT:
            return java.lang.String.class;
        case TIME:
            return java.sql.Time.class;
        case TIMESTAMP:
            return java.sql.Timestamp.class;
        case U_BIGINT:
            return java.math.BigInteger.class;
        case U_DOUBLE:
            return java.math.BigDecimal.class;
        case U_FLOAT:
            return Double.TYPE;
        case U_INT:
            return Long.TYPE;
        case VARBINARY:
            return byte[].class;
        case YEAR:
            return Integer.TYPE;
        case BOOL:
            return Boolean.TYPE;
        case INTERVAL_MILLIS:
            return Long.TYPE;
        case INTERVAL_MONTH:
            return Long.TYPE;
        case RESULT_SET:
            return java.sql.ResultSet.class;
        default:
            throw new UnsupportedOperationException("No support for datatype " + type);
        }

    }

    private String buildDirectIterableExpr(final String className, final String table) {
        return String.format("(new com.akiban.direct.DirectIterableImpl" + "(%1$s.class, \"%2$s\"))", className, table);
    }

    /**
     * Box by hand since the javassist compiler doesn't seem to do it.
     * 
     * @param getterMethods
     * @param column
     * @return
     */
    private String literal(Map<String, String> getterMethods, Column column) {
        String getter = getterMethods.get(column.getName());
        Class<?> type = javaClass(column);
        if (type.isPrimitive()) {
            return literalWrapper(type) + ".valueOf(" + getter + ")";
        } else {
            return getter;
        }
    }

    /*
     * What a kludge!
     */
    private String literalWrapper(final Class<?> c) {
        assert c.isPrimitive();
        if (c == byte.class) {
            return "Byte";
        }
        if (c == char.class) {
            return "Character";
        }
        if (c == short.class) {
            return "Short";
        }
        if (c == int.class) {
            return "Integer";
        }
        if (c == long.class) {
            return "Long";
        }
        if (c == float.class) {
            return "Float";
        }
        if (c == double.class) {
            return "Double";
        }
        if (c == boolean.class) {
            return "Boolean";
        }
        return "?";
    }
}