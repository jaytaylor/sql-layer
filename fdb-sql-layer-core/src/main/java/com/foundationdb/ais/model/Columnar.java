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

package com.foundationdb.ais.model;

import com.foundationdb.ais.model.validation.AISInvariants;
import com.foundationdb.server.types.common.types.StringFactory;

import java.util.*;

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
        return columnMap.get(columnName);
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

    public int getCharsetId()
    {
        return charsetId;
    }

    public String getCharsetName()
    {
        return StringFactory.charsetIdToName(charsetId);
    }

    public int getDefaultedCharsetId()
    {
        return
            charsetId == StringFactory.NULL_CHARSET_ID
            ? ais.getCharsetId() // schema.getDefaultedCharsetId()
            : charsetId;
    }

    public String getDefaultedCharsetName()
    {
        return StringFactory.charsetIdToName(getDefaultedCharsetId());
    }

    public int getCollationId()
    {
        return collationId;
    }

    public String getCollationName()
    {
        return StringFactory.collationIdToName(collationId);
    }

    public int getDefaultedCollationId()
    {
        return
            collationId == StringFactory.NULL_COLLATION_ID
            ? ais.getCollationId() // schema.getDefaultedCollationId()
            : collationId;
    }

    public String getDefaultedCollationName()
    {
        return StringFactory.collationIdToName(getDefaultedCollationId());
    }

    public void setCharsetAndCollation(String charsetName, String collationName)
    {
        charsetId = StringFactory.charsetNameToId(charsetName);
        collationId = StringFactory.collationNameToId(collationName);
    }

    public boolean isAISTable()
    {
        return tableName.getSchemaName().equals(TableName.INFORMATION_SCHEMA);
    }
    
    public boolean isProtectedTable() {
        return tableName.inSystemSchema();
    }

    /** A BitSet where the field positions of all {@code NOT NULL} columns are set. */
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
        columnMap.put(column.getName(), column);
        columnsStale = true;
    }

    public void dropColumns()
    {
        columnMap.clear();
        columnsStale = true;
    }

    void markColumnsStale() {
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
                        if (!column.isInternalColumn()) {
                            columnsWithoutInternal.add(column);
                        }
                        assert (column.getNullable() != null) : column;
                        notNull.set(column.getPosition(), !column.getNullable());
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

    protected int charsetId = StringFactory.NULL_CHARSET_ID;
    protected int collationId = StringFactory.NULL_COLLATION_ID;
}
