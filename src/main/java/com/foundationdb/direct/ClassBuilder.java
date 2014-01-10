/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.foundationdb.direct;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.PrimaryKey;
import com.foundationdb.ais.model.Schema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.aksql.aktypes.AkResultSet;
import com.sun.jersey.core.impl.provider.entity.Inflector;

public abstract class ClassBuilder {
    final static String PACKAGE = "com.foundationdb.direct.entity";
    final static String[] NONE = {};
    final static String[] IMPORTS = { java.util.Date.class.getName(), com.foundationdb.direct.DirectIterable.class.getName() };
    final static String[] UNSUPPORTED = { "// TODO", "throw new UnsupportedOperationException()" };

    private final static Inflector INFLECTOR = Inflector.getInstance();

    public abstract void startClass(String name, boolean isInterface, String extendsClass, String[] implementsClasses,
            String[] imports) throws CannotCompileException, NotFoundException;

    public abstract void addMethod(String name, String returnType, String[] argumentTypes, String[] argumentNames,
            String[] body);

    public abstract void addStaticInitializer(final String body);

    /*
     * (non-Javadoc)
     * 
     * @see com.foundationdb.direct.ClassBuilder#property(java.lang.String,
     * java.lang.String)
     */
    public String addProperty(final String name, final String type, final String argName, final String[] getBody,
            final String[] setBody, final boolean hasSetter) {
        String caseConverted = asJavaName(name, true);
        boolean b = "boolean".equals(type);
        String getName = (b ? "is" : "get") + caseConverted;
        addMethod((b ? "is" : "get") + caseConverted, type, new String[0], null, getBody);
        if (hasSetter) {
            addMethod("set" + caseConverted, "void", new String[] { type }, new String[] { argName == null ? "v"
                    : argName }, setBody);
        }
        return getName + "()";
    }

    public abstract void end();

    public abstract void close();

    public static String asJavaName(final String name, final boolean toUpper) {
        return sanitize(INFLECTOR.camelize(INFLECTOR.singularize(name), !toUpper));
    }

    public static String asJavaCollectionName(final String name, final boolean toUpper) {
        return sanitize(INFLECTOR.camelize(name, !toUpper));
    }

    public static String schemaClassName(String schema) {
        return PACKAGE + "." + sanitize(INFLECTOR.classify(schema));
    }

    public static String sanitize(final String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (Character.isJavaIdentifierPart(ch)) {
                if (i == 0 && !Character.isJavaIdentifierStart(ch)) {
                    sb.append("_");
                }
                sb.append(ch);
            } else {
                sb.append(String.format("_u%04x", (int) ch));
            }
        }
        return sb.toString();
    }

    public static Map<Integer, CtClass> compileGeneratedInterfacesAndClasses(final AkibanInformationSchema ais,
            final String schema) throws CannotCompileException, NotFoundException {
        /*
         * Precompile the interfaces
         */
        ClassPool pool = new ClassPool(true);
        ClassObjectWriter helper = new ClassObjectWriter(pool);
        String scn = schemaClassName(schema);
        Map<Integer, CtClass> implClassMap = new TreeMap<Integer, CtClass>();

        helper.startClass(scn, true, null, null, IMPORTS);
        for (final Table table : ais.getSchema(schema).getTables().values()) {
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

        for (final Table table : ais.getSchema(schema).getTables().values()) {
            helper.generateImplementationClass(table, schema);
            implClassMap.put(table.getTableId(), helper.getCurrentClass());
        }
        /*
         * Precompile the extent class
         */
        helper.startExtentClass(schema, scn);
        for (final Table table : ais.getSchema(schema).getTables().values()) {
            if (table.isRoot()) {
                helper.addExtentAccessor(table, scn, false);
            }
        }
        helper.end();
        implClassMap.put(Integer.MAX_VALUE, helper.getCurrentClass());

        return implClassMap;
    }

    public void writeGeneratedInterfaces(final AkibanInformationSchema ais, final TableName tableName)
            throws CannotCompileException, NotFoundException {
        final String schema = tableName.getSchemaName();
        String scn = schemaClassName(schema);
        startClass(scn, true, null, null, IMPORTS);
        if ("*".equals(tableName.getTableName())) {
            for (final Table table : ais.getSchema(schema).getTables().values()) {
                if (table.isRoot()) {
                    addExtentAccessor(table, scn, true);
                }
                generateInterfaceClass(table, scn);
            }
        } else {
            final Table table = ais.getTable(tableName);
            if (table == null) {
                throw new NoSuchTableException(tableName);
            }
            if (table.isRoot()) {
                addExtentAccessor(table, scn, true);
            }
            generateInterfaceClass(table, scn);
        }
        end();
    }

    public void writeGeneratedClass(final AkibanInformationSchema ais, final TableName tableName)
            throws CannotCompileException, NotFoundException {
        if ("*".equals(tableName.getTableName())) {
            Schema schema = ais.getSchema(tableName.getSchemaName());
            for (final Table table : schema.getTables().values()) {
                generateImplementationClass(table, tableName.getSchemaName());
            }
            String scn = schemaClassName(tableName.getSchemaName());
            startExtentClass(tableName.getSchemaName(), scn);
            for (final Table table : schema.getTables().values()) {
                if (table.isRoot()) {
                    addExtentAccessor(table, scn, false);
                }
            }
            end();

        } else {
            Table table = ais.getTable(tableName);
            if (table == null) {
                throw new NoSuchTableException(tableName);
            }
            generateImplementationClass(table, tableName.getSchemaName());
        }
    }

    public void writeGeneratedXrefs(final AkibanInformationSchema ais, final TableName tableName)
            throws CannotCompileException, NotFoundException {
        final String schema = tableName.getSchemaName();
        String scn = schemaClassName(schema);
        if ("*".equals(tableName.getTableName())) {
            for (final Table table : ais.getSchema(schema).getTables().values()) {
                generateInterfaceClass(table, scn);
            }
            startClass("$$$extent$$$", true, null, null, null);
            for (final Table table : ais.getSchema(schema).getTables().values()) {
                if (table.isRoot()) {
                    addExtentAccessor(table, scn, false);
                }
            }
            end();

        } else {
            final Table table = ais.getTable(tableName);
            if (table == null) {
                throw new NoSuchTableException(tableName);
            }
            generateInterfaceClass(table, scn);
        }
    }

    void generateInterfaceClass(Table table, String scn) throws CannotCompileException, NotFoundException {
        table.getName().getTableName();
        String typeName = scn + "$" + asJavaName(table.getName().getTableName(), true);
        startClass(typeName, true, null, null, null);
        addMethods(table, scn, typeName, typeName, true);
        addMethod("save", "void", NONE, null, null);
        end();
    }

    void generateImplementationClass(Table table, String schemaName) throws CannotCompileException,
            NotFoundException {
        table.getName().getTableName();
        String scn = schemaClassName(schemaName);
        String typeName = scn + "$" + asJavaName(table.getName().getTableName(), true);
        String className = PACKAGE + ".$$$" + asJavaName(schemaName, true) + "$$$"
                + asJavaName(table.getName().getTableName(), true);
        startClass(className, false, "com.foundationdb.direct.AbstractDirectObject", new String[] { typeName }, IMPORTS);
        addStaticInitializer(columnMetadataString(table));
        addMethods(table, scn, typeName, className, false);
        end();
    }

    void startExtentClass(String schema, final String scn) throws CannotCompileException, NotFoundException {
        String className = PACKAGE + ".$$" + asJavaName(schema, true);
        startClass(className, false, "com.foundationdb.direct.AbstractDirectObject", new String[] { scn }, IMPORTS);
        addStaticInitializer(null);
    }

    void addExtentAccessor(Table table, String scn, boolean iface) {
        String tableName = table.getName().getTableName();
        String className = scn + "$" + asJavaName(tableName, true);

        String[] body = null;
        if (!iface) {
            StringBuilder sb = new StringBuilder(buildDirectIterableExpr(className, table));
            body = new String[] { "return " + sb.toString() };
        }
        addMethod("get" + asJavaCollectionName(tableName, true), "com.foundationdb.direct.DirectIterable<" + className + ">",
                NONE, null, body);

        PrimaryKey pk = table.getPrimaryKey();
        if (pk != null) {
            List<Column> primaryKeyColumns = pk.getColumns();
            if (primaryKeyColumns.size() <= 1) {
                /*
                 * Add a child accessor by primary key value
                 */
                String[] types = new String[primaryKeyColumns.size()];
                String[] names = new String[primaryKeyColumns.size()];
                for (int i = 0; i < types.length; i++) {
                    types[i] = javaClass(primaryKeyColumns.get(i)).getName();
                    names[i] = asJavaName(primaryKeyColumns.get(i).getName(), false);
                }

                StringBuilder sb = new StringBuilder(buildDirectIterableExpr(className, table));
                for (int i = 0; i < primaryKeyColumns.size(); i++) {
                    sb.append(String.format(".where(\"%s\", %s)", primaryKeyColumns.get(i).getName(),
                            literal(javaClass(primaryKeyColumns.get(i)), "$" + (i + 1)), false));
                }
                sb.append(".single()");
                body = new String[] { "return " + sb.toString() };

                addMethod("get" + asJavaName(tableName, true), className, types, names, iface ? null : body);
            }
        }
    }

    private void addMethods(Table table, String scn, String typeName, String className, boolean iface) {
        /*
         * Add a property per column
         */
        Map<String, String> getterMethods = new HashMap<String, String>();
        for (final Column column : table.getColumns()) {
            Class<?> javaClass = javaClass(column);
            String[] getBody = new String[] { "return __get" + accessorName(column) + "(" + column.getPosition()
                    + ")" };
            final String paramName = asJavaName(column.getName(), false);
            String[] setBody = new String[] { "__set" + accessorName(column) + "(" + column.getPosition() + ",$1)" };
            String expr = addProperty(column.getName(), javaClass.getName(), paramName, iface ? null : getBody,
                    iface ? null : setBody, true);
            getterMethods.put(column.getName(), expr);
        }

        /*
         * Add an accessor for the parent row if there is one
         */
        Join parentJoin = table.getParentJoin();
        if (parentJoin != null) {
            Table parentTable = parentJoin.getParent();
            String parentTableName = parentTable.getName().getTableName();
            String parentClassName = scn + "$" + asJavaName(parentTableName, true);
            String[] body = null;
            if (!iface) {
                StringBuilder sb = new StringBuilder(buildDirectIterableExpr(parentClassName, parentTable));
                for (final JoinColumn jc : parentJoin.getJoinColumns()) {
                    sb.append(String.format(".where(\"%s\", %s)", jc.getParent().getName(),
                            literal(getterMethods, jc.getParent())));
                }
                body = new String[] { "return " + sb.toString() + ".single()" };
            }
            addProperty(asJavaName(parentTableName, false), parentClassName, null, body, NONE, false);
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
                String[] names = new String[primaryKeyColumns.size()];
                for (int i = 0; i < types.length; i++) {
                    types[i] = javaClass(primaryKeyColumns.get(i)).getName();
                    names[i] = asJavaName(primaryKeyColumns.get(i).getName(), false);
                }
                String[] body = null;
                if (!iface) {
                    StringBuilder sb = new StringBuilder(buildDirectIterableExpr(childClassName, join.getChild()));
                    for (final JoinColumn jc : join.getJoinColumns()) {
                        sb.append(String.format(".where(\"%s\", %s)", jc.getChild().getName(),
                                literal(getterMethods, jc.getParent())));
                    }
                    for (int i = 0; i < primaryKeyColumns.size(); i++) {
                        sb.append(String.format(".where(\"%s\", %s)", primaryKeyColumns.get(i).getName(),
                                literal(javaClass(primaryKeyColumns.get(i)), "$" + (i + 1)), false));
                    }
                    sb.append(".single()");
                    body = new String[] { "return " + sb.toString() };
                }
                addMethod("get" + asJavaName(childTableName, true), childClassName, types, names, iface ? null : body);
            }
            if (!primaryKeyColumns.isEmpty()) {
                String[] body = null;
                if (!iface) {
                    StringBuilder sb = new StringBuilder(buildDirectIterableExpr(childClassName, join.getChild()));
                    for (final JoinColumn jc : join.getJoinColumns()) {
                        sb.append(String.format(".where(\"%s\", %s)", jc.getChild().getName(),
                                literal(getterMethods, jc.getParent())));
                    }
                    body = new String[] { "return " + sb.toString() };
                }
                addMethod("get" + asJavaCollectionName(childTableName, true), "com.foundationdb.direct.DirectIterable<"
                        + childClassName + ">", NONE, null, body);
            }
        }
    }

    /**
     * Generate a Java command that will be executed as a static initializer and
     * will give the base class metadata about the columns.
     */
    @SuppressWarnings("unchecked")
    private String columnMetadataString(final Table table) {
        String[] columnArray = new String[table == null ? 0 : table.getColumns().size()];
        if (table != null) {
            PrimaryKey pk = table.getPrimaryKey();
            List<Column> pkColumns = pk == null ? Collections.EMPTY_LIST : pk.getColumns();
            List<JoinColumn> joinColumns = table.getParentJoin() == null ? Collections.EMPTY_LIST : table
                    .getParentJoin().getJoinColumns();
            for (Column column : table.getColumns()) {
                int index = column.getPosition();
                String columnName = column.getName();
                String propertyName = asJavaName(columnName, false);
                String type = javaClass(column).getSimpleName();
                int pkFieldIndex = -1;
                int pjFieldIndex = -1;
                for (int pkindex = 0; pkindex < pkColumns.size(); pkindex++) {
                    if (pkColumns.get(pkindex) == column) {
                        pkFieldIndex = pkindex;
                        break;
                    }
                }
                for (JoinColumn jc : joinColumns) {
                    if (jc.getChild() == column) {
                        pjFieldIndex = jc.getParent().getPosition();
                        break;
                    }
                }
                columnArray[index] = String.format("%s:%s:%s:%d:%d", columnName, propertyName, type, pkFieldIndex,
                        pjFieldIndex);
            }
        }
        StringBuilder sb = new StringBuilder("__init(");
        sb.append("\"").append(table.getName().getSchemaName()).append("\", ");
        sb.append("\"").append(table.getName().getTableName()).append("\", ");
        sb.append("\"");
        for (int index = 0; index < columnArray.length; index++) {
            assert columnArray[index] != null : "Missing column specification: " + index;
            if (index > 0) {
                sb.append(',');
            }
            sb.append(columnArray[index]);
        }
        sb.append("\")");
        return sb.toString();
    }

    private String accessorName(final Column column) {
        TClass tclass = column.getType().typeClass();
        int jdbcType = tclass.jdbcType();
        switch (jdbcType) {
        case Types.DECIMAL:
        case Types.NUMERIC:
            return "BigDecimal";
        case Types.BOOLEAN:
            return "Boolean";
        case Types.TINYINT:
            return "Byte";
        case Types.BINARY:
        case Types.BIT:
        case Types.LONGVARBINARY:
        case Types.VARBINARY:
        case Types.BLOB:
            return "Bytes";
        case Types.DATE:
            return "Date";
        case Types.DOUBLE:
            return "Double";
        case Types.FLOAT:
        case Types.REAL:
            return "Float";
        case Types.INTEGER:
            return "Int";
        case Types.BIGINT:
            return "Long";
        case Types.SMALLINT:
            return "Short";
        case Types.CHAR:
        case Types.LONGNVARCHAR:
        case Types.LONGVARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.VARCHAR:
        case Types.CLOB:
            return "String";
        case Types.TIME:
            return "Time";
        case Types.TIMESTAMP:
            return "Timestamp";

        /*
        case Types.ARRAY:
            return "Array";
        case Types.BLOB:
            return "Blob";
        case Types.CLOB:
            return "Clob";
        case Types.NCLOB:
            return "NClob";
        case Types.LONGNVARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
            return "NString";
        case Types.REF:
            return "Ref";
        case Types.ROWID:
            return "RowId";
        case Types.SQLXML:
            return "SQLXML";
        */

        case Types.NULL:
        case Types.DATALINK:
        case Types.DISTINCT:
        case Types.JAVA_OBJECT:
        case Types.OTHER:
        case Types.STRUCT:
        default:
            return "Object";
        }
    }

    private Class<?> javaClass(final Column column) {
        TClass tclass = column.getType().typeClass();
        int jdbcType = tclass.jdbcType();
        // Similar to TypesTranslator.jdbcClass(), but returning primitives not wrappers.
        switch (jdbcType) {
        case Types.DECIMAL:
        case Types.NUMERIC:
            return java.math.BigDecimal.class;
        case Types.BOOLEAN:
            return Boolean.TYPE;
        case Types.TINYINT:
            return Byte.TYPE;
        case Types.BINARY:
        case Types.BIT:
        case Types.LONGVARBINARY:
        case Types.VARBINARY:
        case Types.BLOB:
            return byte[].class;
        case Types.DATE:
            return java.sql.Date.class;
        case Types.DOUBLE:
            return Double.TYPE;
        case Types.FLOAT:
        case Types.REAL:
            return Float.TYPE;
        case Types.INTEGER:
            return Integer.TYPE;
        case Types.BIGINT:
            return Long.TYPE;
        case Types.SMALLINT:
            return Short.TYPE;
        case Types.CHAR:
        case Types.LONGNVARCHAR:
        case Types.LONGVARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.VARCHAR:
        case Types.CLOB:
            return String.class;
        case Types.TIME:
            return java.sql.Time.class;
        case Types.TIMESTAMP:
            return java.sql.Timestamp.class;

        /*
        case Types.ARRAY:
            return java.sql.Array.class;
        case Types.BLOB:
            return java.sql.Blob.class;
        case Types.CLOB:
            return java.sql.Clob.class;
        case Types.NCLOB:
            return java.sql.NClob.class;
        case Types.REF:
            return java.sql.Ref.class;
        case Types.ROWID:
            return java.sql.RowId.class;
        case Types.SQLXML:
            return java.sql.SQLXML.class;
        */

        case Types.NULL:
        case Types.DATALINK:
        case Types.DISTINCT:
        case Types.JAVA_OBJECT:
        case Types.OTHER:
        case Types.STRUCT:
        default:
            break;
        }

        if (tclass == AkResultSet.INSTANCE) {
            return java.sql.ResultSet.class;
        }
        return Object.class;
    }

    private String buildDirectIterableExpr(final String className, final Table table) {
        return String.format("(new com.foundationdb.direct.DirectIterableImpl" + "(%s.class, \"%s\", this))",
                className, table.getName().getTableName());
    }

    /**
     * Box by hand since the javassist compiler doesn't seem to do it.
     * 
     * @param getterMethods
     * @param column
     * @return
     */
    private String literal(Map<String, String> getterMethods, Column column) {
        return literal(javaClass(column), getterMethods.get(column.getName()));
    }

    private String literal(Class<?> type, final String getter) {
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
