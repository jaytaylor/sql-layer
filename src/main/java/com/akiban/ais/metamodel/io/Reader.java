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

package com.akiban.ais.metamodel.io;

import java.util.Map;

import com.akiban.ais.metamodel.MMColumn;
import com.akiban.ais.metamodel.MMGroup;
import com.akiban.ais.metamodel.MMIndex;
import com.akiban.ais.metamodel.MMIndexColumn;
import com.akiban.ais.metamodel.MMJoin;
import com.akiban.ais.metamodel.MMJoinColumn;
import com.akiban.ais.metamodel.MMTable;
import com.akiban.ais.metamodel.MMType;
import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.Source;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
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
                MMTable.create(ais, map);
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
                MMJoin.create(ais, map);
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
                MMJoinColumn.create(ais, map);
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
                IndexColumn indexColumn = MMIndexColumn.create(ais, map);
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
