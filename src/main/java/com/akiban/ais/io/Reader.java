/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais.io;

import java.util.Map;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Source;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;

public class Reader
{
    private AkibanInformationSchema ais;
    private final Source source;
    private final ArraySource cache = new ArraySource();

    public Reader(Source source)
    {
        this.source = source;
    }

    private void loadGroups() throws Exception
    {
        source.readGroups(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map) throws Exception
            {
                Group.create(ais, map);
            }
        });
    }

    private void loadTables() throws Exception
    {
        source.readTables(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map) throws Exception
            {
                Table table = Table.create(ais, map);
                if (table == null) {
                    throw new ReaderException(String.format("invalid table type (%s) for %s.%s",
                                                            map.get(table_tableType),
                                                            map.get(table_schemaName),
                                                            map.get(table_tableName)));
                }
            }
        });
    }

    private void loadColumns() throws Exception
    {
        source.readColumns(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map) throws Exception
            {
                Column column = Column.create(ais, map);
                if (column == null) {
                    throw new ReaderException(String.format("Unknown table %s.%s.",
                                                            map.get(column_schemaName),
                                                            map.get(column_tableName)));
                }
                cache.addColumn(map);
            }
        });
        loadUserGroupColumnConnections();
    }

    private void loadUserGroupColumnConnections() throws Exception
    {
        cache.readColumns(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map) throws ReaderException
            {
                String schemaName = (String) map.get(column_schemaName);
                String tableName = (String) map.get(column_tableName);
                String columnName = (String) map.get(column_columnName);
                String groupSchemaName = (String) map.get(column_groupSchemaName);
                String groupTableName = (String) map.get(column_groupTableName);
                String groupColumnName = (String) map.get(column_groupColumnName);
                UserTable table = ais.getUserTable(schemaName, tableName);
                if (table != null && groupSchemaName != null) {
                    // group_* columns describe mapping from user table columns to group table columns.
                    UserTable userTable = table;
                    Column userColumn = userTable.getColumn(columnName);
                    GroupTable groupTable = ais.getGroupTable(groupSchemaName, groupTableName);
                    Column groupColumn = groupTable.getColumn(groupColumnName);
                    userColumn.setGroupColumn(groupColumn);
                    groupColumn.setUserColumn(userColumn);
                }
            }
        });
    }

    private void loadJoins() throws Exception
    {
        source.readJoins(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map)
            {
                Join.create(ais, map);
            }
        });
    }

    private void loadJoinColumns() throws Exception
    {
        source.readJoinColumns(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map)
            {
                JoinColumn.create(ais, map);
            }
        });
    }

    private void loadIndexes() throws Exception
    {
        source.readIndexes(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map) throws ReaderException
            {
                Index index = Index.create(ais, map);
                if (index == null) {
                    throw new ReaderException(String.format("No Table named %s.%s.",
                                                            map.get(index_schemaName),
                                                            map.get(index_tableName)));
                }
            }
        });
    }

    private void loadIndexColumns() throws Exception
    {
        source.readIndexColumns(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map) throws ReaderException
            {
                IndexColumn indexColumn = IndexColumn.create(ais, map);
                if (indexColumn == null) {
                    throw new ReaderException(String.format("Unable to find table, index or column while creating IndexColumn: %s", map));
                }
            }
        });
    }

    private void loadTypes() throws Exception
    {
        source.readTypes(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map) throws Exception
            {
                Type.create(ais, map);
            }
        });
    }

    protected void close() throws Exception
    {
        // Need to call UserTable.endTable to get PKs created properly (in the case that no PK was declared). This isn't
        // so easy from DDLSource, at least with DDLSource in its current form. It should be harmless to call
        // endTable here, if a PK exists (possibly due to a prior call of endTable). But if it hasn't been called,
        // this is a good time to do it.
        for (UserTable userTable : ais.getUserTables().values()) {
            userTable.endTable();
        }
        source.close();
    }

    public AkibanInformationSchema load() throws Exception
    {
        load(new AkibanInformationSchema());
        return ais;
    }

    public AkibanInformationSchema load(final AkibanInformationSchema ais) throws Exception
    {
    	this.ais = ais;
        try {
            loadTypes();
            loadGroups();
            loadTables();
            loadColumns();
            loadJoins();
            loadJoinColumns();
            loadIndexes();
            loadIndexColumns();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            close();
        }
        return ais;
    }

    public static class ReaderException extends Exception
    {
        public ReaderException(String message)
        {
            super(message);
        }
    }
}
