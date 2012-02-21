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

import com.akiban.ais.metamodel.MMColumn;
import com.akiban.ais.metamodel.MMGroup;
import com.akiban.ais.metamodel.MMIndex;
import com.akiban.ais.metamodel.MMType;
import com.akiban.ais.metamodel.MetaModel;
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
import com.akiban.server.error.MetaModelVersionMismatchException;
import com.akiban.server.error.NoSuchTableException;

public class Reader
{
    private AkibanInformationSchema ais;
    private final Source source;
    private final ArraySource cache = new ArraySource();

    public Reader(Source source)
    {
        this.source = source;
    }

    private void loadGroups() 
    {
        source.readGroups(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map)
            {
                MMGroup.create(ais, map);
            }
        });
    }

    private void loadTables() 
    {
        source.readTables(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map)
            {
                Table.create(ais, map);
            }
        });
    }

    private void loadColumns() 
    {
        source.readColumns(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map)
            {
                Column column = MMColumn.create(ais, map);
                if (column == null) {
                    throw new NoSuchTableException ((String)map.get(column_schemaName), (String)map.get(column_tableName));
                }
                cache.addColumn(map);
            }
        });
        loadUserGroupColumnConnections();
    }

    private void loadUserGroupColumnConnections() 
    {
        cache.readColumns(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map)
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

    private void loadJoins()
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

    private void loadJoinColumns() 
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

    private void loadIndexes()
    {
        source.readIndexes(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map)
            {
                Index index = MMIndex.create(ais, map);
                if (index == null) {
                    throw new NoSuchTableException ((String)map.get(index_schemaName), 
                                                (String)map.get(index_tableName));
                }
            }
        });
    }

    private void loadIndexColumns()
    {
        source.readIndexColumns(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map)
            {
                IndexColumn indexColumn = IndexColumn.create(ais, map);
                if (indexColumn == null) {
                    throw new NoSuchTableException ((String)map.get(indexColumn_schemaName), (String)map.get(indexColumn_tableName));
                }
            }
        });
    }

    private void loadTypes()
    {
        source.readTypes(new Source.Receiver()
        {
            @Override
            public void receive(Map<String, Object> map)
            {
                MMType.create(ais, map);
            }
        });
    }
    
    private void loadVersion()
    {
        final int currentVersion = MetaModel.only().getModelVersion();
        final int sourceVersion = source.readVersion();
        if (sourceVersion != currentVersion) {
            throw new MetaModelVersionMismatchException();
        }
    }

    protected void close()
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

    public AkibanInformationSchema load()
    {
        load(new AkibanInformationSchema());
        return ais;
    }

    public AkibanInformationSchema load(final AkibanInformationSchema ais)
    {
    	this.ais = ais;
        try {
            loadVersion();
            loadTypes();
            loadGroups();
            loadTables();
            loadColumns();
            loadJoins();
            loadJoinColumns();
            loadIndexes();
            loadIndexColumns();
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
