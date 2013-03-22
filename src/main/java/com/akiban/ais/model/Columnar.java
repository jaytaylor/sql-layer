
package com.akiban.ais.model;

import java.util.*;

import com.akiban.ais.model.validation.AISInvariants;

/** Common base class for tables and views, which both have columns. */
public abstract class Columnar
{
    public abstract boolean isView();

    public boolean isTable() {
        return !isView();
    }

    @Override
    public String toString()
    {
        return tableName.toString();
    }

    protected Columnar(AkibanInformationSchema ais, String schemaName, String tableName)
    {
        ais.checkMutability();
        AISInvariants.checkNullName(schemaName, "Table", "schema name");
        AISInvariants.checkNullName(tableName, "Table", "table name");
        AISInvariants.checkDuplicateTables(ais, schemaName, tableName);

        this.ais = ais;
        this.tableName = new TableName(schemaName, tableName);
    }

    public AkibanInformationSchema getAIS()
    {
        return ais;
    }

    public TableName getName()
    {
        return tableName;
    }

    public Column getColumn(String columnName)
    {
        return columnMap.get(columnName.toLowerCase());
    }

    public Column getColumn(Integer position)
    {
        return getColumns().get(position);
    }

    public List<Column> getColumns()
    {
        ensureColumnsUpToDate();
        return columnsWithoutInternal;
    }

    public List<Column> getColumnsIncludingInternal()
    {
        ensureColumnsUpToDate();
        return columns;
    }

    public CharsetAndCollation getCharsetAndCollation()
    {
        return
            charsetAndCollation == null
            ? ais.getCharsetAndCollation()
            : charsetAndCollation;
    }

    public void setCharsetAndCollation(CharsetAndCollation charsetAndCollation)
    {
        this.charsetAndCollation = charsetAndCollation;
    }

    public void setCharset(String charset)
    {
        if (charset != null) {
            this.charsetAndCollation = CharsetAndCollation.intern(charset, getCharsetAndCollation().collation());
        }
    }

    public void setCollation(String collation)
    {
        if (collation != null) {
            this.charsetAndCollation = CharsetAndCollation.intern(getCharsetAndCollation().charset(), collation);
        }
    }

    public boolean isAISTable()
    {
        return tableName.getSchemaName().equals(TableName.INFORMATION_SCHEMA);
    }

    public BitSet notNull()
    {
        ensureColumnsUpToDate();
        return notNull;
    }

    public Column dropColumn(String columnName)
    {
        columnsStale = true;
        return columnMap.remove(columnName);
    }

    protected void addColumn(Column column)
    {
        columnMap.put(column.getName().toLowerCase(), column);
        columnsStale = true;
    }

    protected void dropColumns()
    {
        columnMap.clear();
        columnsStale = true;
    }

    // For use by this package

    void setTableName(TableName tableName)
    {
        this.tableName = tableName;
    }

    /**
     * check if this table belongs to a frozen AIS, 
     * throw exception if ais is frozen 
     */
    void checkMutability() {
        ais.checkMutability();
    }

    private void ensureColumnsUpToDate()
    {
        if (columnsStale) {
            synchronized (columnsStaleLock) {
                if (columnsStale) {
                    columns.clear();
                    columns.addAll(columnMap.values());
                    Collections.sort(columns,
                                     new Comparator<Column>()
                                     {
                                         @Override
                                         public int compare(Column x, Column y)
                                         {
                                             return x.getPosition() - y.getPosition();
                                         }
                                     });
                    columnsWithoutInternal.clear();
                    notNull = new BitSet(columns.size());
                    for (Column column : columns) {
                        if (!column.isAkibanPKColumn()) {
                            columnsWithoutInternal.add(column);
                        }
                        Boolean nullable = column.getNullable();
                        notNull.set(column.getPosition(), nullable != null && !nullable);
                    }
                    columnsStale = false;
                }
            }
        }
    }

    // State
    protected final AkibanInformationSchema ais;
    protected final Object columnsStaleLock = new Object();
    protected final List<Column> columns = new ArrayList<>();
    protected final List<Column> columnsWithoutInternal = new ArrayList<>();
    protected final Map<String, Column> columnMap = new TreeMap<>();
    private BitSet notNull;

    protected TableName tableName;
    protected volatile boolean columnsStale = true;
    protected CharsetAndCollation charsetAndCollation;
}
