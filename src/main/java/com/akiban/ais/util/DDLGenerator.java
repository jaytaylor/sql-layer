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

package com.akiban.ais.util;

import java.util.ArrayList;
import java.util.List;

import com.akiban.ais.model.CharsetAndCollation;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.Types;
import com.akiban.ais.model.UserTable;

public class DDLGenerator
{
    private final String useSchemaName;
    private final String useTableName;


    public DDLGenerator() {
        this(null,null);
    }

    public DDLGenerator(final String schemaName, final String tableName) {
        this.useSchemaName = schemaName;
        this.useTableName = tableName;
    }

    public String createTable(Table table)
    {
        // columns
        List<String> columnDeclarations = new ArrayList<String>();
        for (Column column : table.getColumns()) {
            columnDeclarations.add(declaration(column));
        } 
        // indexes
        String pkeyDecl = null;
        List<String> indexDecls = new ArrayList<String>();
        List<String> fkeyDecls = new ArrayList<String>();
        for (TableIndex index : table.getIndexes()) {
            String decl = declaration(index);
            if(index.isPrimaryKey())  {
                pkeyDecl = decl;
            }
            else if(index.getConstraint().equals("FOREIGN KEY")) {
                fkeyDecls.add(decl);
            }
            else {
                indexDecls.add(decl);
            }
        }
        // generate ddl
        final TableName tname = table.getName();
        final String schemaName = useSchemaName != null ? useSchemaName : tname.getSchemaName();
        final String tableName = useTableName != null ? useTableName : tname.getTableName();
        return String.format(CREATE_TABLE_TEMPLATE,
                             quote(schemaName),
                             quote(tableName),
                             commaSeparated(columnDeclarations),
                             pkeyDecl == null ? "" : ", " + pkeyDecl,
                             indexDecls.isEmpty() ? "" : ", " + commaSeparated(indexDecls),
                             fkeyDecls.isEmpty() ? "" : ", " + commaSeparated(fkeyDecls),
                             tableOptions(table));
    }

    public String dropTable(Table table)
    {
        return String.format(DROP_TABLE_TEMPLATE,
                             quote(table.getName().getSchemaName()),
                             quote(table.getName().getTableName()));
    }

    private String declaration(Column column)
    {
        StringBuilder declaration = new StringBuilder();
        declaration.append(quote(column.getName()));
        declaration.append(' ');
        Type type = column.getType();
        final String typeName;
        final boolean typeIsUnsigned;
        if (type.name().endsWith(" unsigned")) {
            int spaceIndex = type.name().indexOf(' ');
            typeName = type.name().substring(0, spaceIndex);
            typeIsUnsigned = true;
        }
        else {
            typeName = type.name();
            typeIsUnsigned = false;
        }

        declaration.append(typeName);
        if (type.nTypeParameters() >= 1) {
            declaration.append('(');
            declaration.append(column.getTypeParameter1());
            if (type.nTypeParameters() >= 2) {
                declaration.append(", ");
                declaration.append(column.getTypeParameter2());
            }
            declaration.append(')');
        }
        if (typeIsUnsigned) {
            declaration.append(" unsigned");
        }
        if (Types.isTextType(type)) {
            final CharsetAndCollation charAndCol = column.getCharsetAndCollation();
            final CharsetAndCollation tableCharAndCol = column.getTable().getCharsetAndCollation();
            if (charAndCol.charset() != null &&
                charAndCol.charset().equals(tableCharAndCol.charset()) == false) {
                declaration.append(" CHARACTER SET ");
                declaration.append(charAndCol.charset());
            }
            if (charAndCol.collation() != null &&
                charAndCol.collation().equals(tableCharAndCol.collation()) == false) {
                declaration.append(" COLLATE ");
                declaration.append(charAndCol.collation());
            }
        }
        if (column.getNullable() == false) {
            declaration.append(" NOT NULL");
        }
        if (column.getInitialAutoIncrementValue() != null) {
            declaration.append(" AUTO_INCREMENT");
        }
        return declaration.toString();
    }

    private String declaration(TableIndex index)
    {
        List<String> columnDecls = new ArrayList<String>();
        for (IndexColumn indexColumn : index.getKeyColumns()) {
            columnDecls.add(declaration(indexColumn));
        }
        
        if(index.getConstraint().equals("FOREIGN KEY") && index.getTable().isUserTable()) {
            Join join = ((UserTable)index.getTable()).getParentJoin();
            
            if(join == null) {
                return new String("");
            }
            
            List<String> parentColumnDecls = new ArrayList<String>();
            for (JoinColumn joinColumn : join.getJoinColumns()) {
                parentColumnDecls.add(quote(joinColumn.getParent().getName()));
            }
            
            return String.format("CONSTRAINT %s FOREIGN KEY %s(%s) REFERENCES %s(%s)", 
                                 quote(index.getIndexName().getName()),             
                                 quote(index.getIndexName().getName()),
                                 commaSeparated(columnDecls),
                                 quote(join.getParent().getName().getTableName()),
                                 commaSeparated(parentColumnDecls));
        }
        
        return String.format(INDEX_TEMPLATE,
                             index.isPrimaryKey() ? "PRIMARY KEY" : index.getConstraint(),
                             index.isPrimaryKey() ? "" : " " + quote(index.getIndexName().getName()),
                             commaSeparated(columnDecls));
    }

    private String declaration(IndexColumn indexColumn)
    {
        StringBuilder declaration = new StringBuilder();
        declaration.append(quote(indexColumn.getColumn().getName()));
        if (!indexColumn.isAscending()) {
            declaration.append(" desc");
        }
        return declaration.toString();
    }

    private String tableOptions(Table table) {
        StringBuilder tableOptions = new StringBuilder();
        final String engine = table.getEngine() != null ? table.getEngine() : "akibandb";
        tableOptions.append(" engine=");
        tableOptions.append(engine);
        final CharsetAndCollation charAndCol = table.getCharsetAndCollation();
        tableOptions.append(" DEFAULT CHARSET=");
        tableOptions.append(charAndCol.charset());
        if (charAndCol.collation() != null) {
            tableOptions.append(" COLLATE=");
            tableOptions.append(charAndCol.collation());
        }
        return tableOptions.toString();
    }

    private static String commaSeparated(List<String> list)
    {
        StringBuilder buffer = new StringBuilder();
        boolean first = true;
        for (String s : list) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(s);
        }
        return buffer.toString();
    }

    private static String quote(String s)
    {
        return String.format("`%s`", s);
    }

    // create table. Template arguments:
    // - schema name
    // - table name
    // - column declarations
    // - comma, if there are index declarations
    // - primary key declarations
    // - index declarations
    // - foreign key declarations
    // - table options
    private static final String CREATE_TABLE_TEMPLATE = "create table %s.%s(%s%s%s%s)%s";
    // index declaration in create table statement. Template arguments:
    // - constraint (primary key, key, or unique)
    // - index name
    // - index column declarations (each column may have a 'desc' specifier) 
    private static final String INDEX_TEMPLATE = "%s%s(%s)";
    // drop table. Template arguments:
    // - schema name
    // - table name
    private static final String DROP_TABLE_TEMPLATE = "drop table if exists %s.%s";
}
