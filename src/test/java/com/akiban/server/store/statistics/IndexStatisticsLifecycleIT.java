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

package com.akiban.server.store.statistics;

import com.akiban.sql.pg.PostgresServerFilesITBase;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;

import java.sql.*;
import java.util.*;
import java.io.File;

public class IndexStatisticsLifecycleIT extends PostgresServerFilesITBase
{
    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + PersistitStoreIndexStatisticsIT.class.getPackage().getName().replace('.', '/'));

    @Before
    public void loadDatabase() throws Exception {
        loadDatabase(RESOURCE_DIR);
    }

    protected Statement executeStatement;
    protected PreparedStatement checkStatement;
    protected final String CHECK_SQL = "SELECT header.table_id, header.index_id, COUNT(detail.column_count) AS ndetail FROM akiban_information_schema.zindex_statistics header LEFT JOIN akiban_information_schema.zindex_statistics_entry detail USING (table_id, index_id) GROUP BY header.table_id, header.index_id";

    @Before
    public void prepareStatements() throws Exception {
        executeStatement = connection.createStatement();
        checkStatement = connection.prepareStatement(CHECK_SQL);
    }
    
    // Check what stats are in the database. Do this using the
    // akiban_information_schema instead of any IndexStatistics API so
    // as to detect problems with the loader / caches, etc.
    protected Map<Index,Integer> check() throws Exception {
        Map<Index,Integer> result = new HashMap<Index,Integer>();
        AkibanInformationSchema ais = ddl().getAIS(session());
        ResultSet rs = checkStatement.executeQuery();
        while (rs.next()) {
            int tableId = rs.getInt(1);
            int indexId = rs.getInt(2);
            int count = rs.getInt(3);
            Table table = null;
            Index index = null;
            UserTable userTable = ais.getUserTable(tableId);
            if (userTable != null) {
                table = userTable;
                for (TableIndex tindex : userTable.getIndexesIncludingInternal()) {
                    if (tindex.getIndexId() == indexId) {
                        index = tindex;
                        break;
                    }
                }
            }
            else {
                GroupTable groupTable = ais.getGroupTable(tableId);
                if (groupTable != null) {
                    table = groupTable;
                    for (GroupIndex gindex : groupTable.getGroup().getIndexes()) {
                        if (gindex.getIndexId() == indexId) {
                            index = gindex;
                            break;
                        }
                    }
                }
            }
            assertNotNull("Table id refers to table", table);
            assertNotNull("Index id refers to index", index);
            assertTrue("Stats have some entries", (count > 0));
            if (table.getName().getSchemaName().equals(SCHEMA_NAME))
                result.put(index, count);
        }
        return result;
    }

    @Test
    public void test() throws Exception {
        Map<Index,Integer> entries;

        entries = check();
        assertTrue("No stats before analyze", entries.isEmpty());
        
        executeStatement.executeUpdate("ALTER TABLE parent ALL UPDATE STATISTICS");
        executeStatement.executeUpdate("ALTER TABLE child ALL UPDATE STATISTICS");
        entries = check();
        assertTrue("Some stats before analyze", !entries.isEmpty());
        AkibanInformationSchema ais = ddl().getAIS(session());
        TableIndex parentPK = ais.getTable(SCHEMA_NAME, "parent").getIndex("PRIMARY");
        Integer parentPKCount = entries.get(parentPK);
        assertNotNull("parent PK was analyzed", parentPKCount);
        assertEquals("parent PK two entries", 2, parentPKCount.intValue());
        TableIndex parentName = ais.getTable(SCHEMA_NAME, "parent").getIndex("name");
        Integer parentNameCount = entries.get(parentName);
        assertNotNull("parent name was analyzed", parentNameCount);
        assertEquals("parent name two entries", 2, parentNameCount.intValue());
        GroupIndex bothNames = ais.getGroup("parent").getIndex("names");
        assertNull("group index not analyzed", entries.get(bothNames));
        
        executeStatement.executeUpdate("ALTER TABLE parent UPDATE STATISTICS names");
        entries = check();
        ais = ddl().getAIS(session());
        bothNames = ais.getGroup("parent").getIndex("names");
        Integer bothNamesCount = entries.get(bothNames);
        assertNotNull("group index was analyzed", bothNamesCount);
        assertEquals("group index two entries", 4, bothNamesCount.intValue());

        executeStatement.executeUpdate("DROP INDEX parent.name");
        entries = check();
        ais = ddl().getAIS(session());
        parentPK = ais.getTable(SCHEMA_NAME, "parent").getIndex("PRIMARY");
        bothNames = ais.getGroup("parent").getIndex("names");
        parentPKCount = entries.get(parentPK);
        bothNamesCount = entries.get(bothNames);
        assertEquals("parent PK intact after name drop", 2, parentPKCount.intValue());
        assertEquals("group index intact after name drop", 4, bothNamesCount.intValue());

        executeStatement.executeUpdate("DROP INDEX names");
        entries = check();
        ais = ddl().getAIS(session());
        parentPK = ais.getTable(SCHEMA_NAME, "parent").getIndex("PRIMARY");
        parentPKCount = entries.get(parentPK);
        assertEquals("parent PK intact after group indx drop", 2, parentPKCount.intValue());
        
        executeStatement.executeUpdate("DROP TABLE child");
        entries = check();
        ais = ddl().getAIS(session());
        parentPK = ais.getTable(SCHEMA_NAME, "parent").getIndex("PRIMARY");
        parentPKCount = entries.get(parentPK);
        assertEquals("parent PK intact after child drop", 2, parentPKCount.intValue());

        executeStatement.executeUpdate("DROP TABLE parent");
        entries = check();
        assertTrue("No stats after drop group", entries.isEmpty());
    }

}
