/* <GENERIC-HEADER - BEGIN>
 *
 * $(COMPANY) $(COPYRIGHT)
 *
 * Created on: Nov, 20, 2009
 * Created by: Thomas Hazel
 *
 * </GENERIC-HEADER - END> */

package com.akiban.ais.io;

import com.akiban.ais.model.*;

import java.util.Map;

public class Reader
{
    private AkibaInformationSchema ais;
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
        source.close();
    }

    public AkibaInformationSchema load() throws Exception
    {
        load(new AkibaInformationSchema());
        return ais;
    }

    public AkibaInformationSchema load(final AkibaInformationSchema ais) throws Exception
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
