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

package com.foundationdb.sql.pg;

import com.foundationdb.ais.model.*;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.server.store.statistics.IndexStatisticsYamlTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.*;
import java.io.File;

public class IndexStatisticsLifecycleIT extends PostgresServerFilesITBase
{
    public static final File RESOURCE_DIR = new File(PostgresServerITBase.RESOURCE_DIR, "stats");

    @Before
    public void loadDatabase() throws Exception {
        loadDatabase(RESOURCE_DIR);
    }

    protected Statement executeStatement;
    protected Statement checkStatement;
    protected final String CHECK_SQL = "SELECT header.table_id, header.index_id, COUNT(detail.column_count) AS ndetail FROM "+
            IndexStatisticsService.INDEX_STATISTICS_TABLE_NAME.getDescription() + " header LEFT JOIN " +
            IndexStatisticsService.INDEX_STATISTICS_ENTRY_TABLE_NAME.getDescription() + " detail USING (table_id, index_id) GROUP BY header.table_id, header.index_id";

    @Before
    public void prepareStatements() throws Exception {
        executeStatement = getConnection().createStatement();
        checkStatement = getConnection().createStatement();
    }
    
    @After
    public void closeStatements() throws Exception {
        checkStatement.close();
        executeStatement.close();
    }

    // Check what stats are in the database. Do this using the
    // information_schema instead of any IndexStatistics API so
    // as to detect problems with the loader / caches, etc.
    protected Map<Index,Integer> check() throws Exception {
        Map<Index,Integer> result = new HashMap<>();
        AkibanInformationSchema ais = ddl().getAIS(session());
        ResultSet rs = checkStatement.executeQuery(CHECK_SQL);
        while (rs.next()) {
            int tableId = rs.getInt(1);
            int indexId = rs.getInt(2);
            int count = rs.getInt(3);
            Table table = null;
            Index index = null;
            Table aTable = ais.getTable(tableId);
            if (aTable != null) {
                table = aTable;
                for (TableIndex tindex : aTable.getIndexesIncludingInternal()) {
                    if (tindex.getIndexId() == indexId) {
                        index = tindex;
                        break;
                    }
                }
                if (index == null) {
                    for (GroupIndex gindex : aTable.getGroupIndexes()) {
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
        GroupIndex bothValue = ais.getGroup(new TableName(SCHEMA_NAME, "parent")).getIndex("value");
        Integer bothValueCount = entries.get(bothValue);
        assertNotNull("group index was analyzed", bothValueCount);
        assertEquals("group index two entries", 6, bothValueCount.intValue());

        executeStatement.executeUpdate("DROP INDEX parent.name");
        entries = check();
        ais = ddl().getAIS(session());
        parentPK = ais.getTable(SCHEMA_NAME, "parent").getIndex("PRIMARY");
        bothValue = ais.getGroup(new TableName(SCHEMA_NAME , "parent")).getIndex("value");
        parentPKCount = entries.get(parentPK);
        bothValueCount = entries.get(bothValue);
        assertEquals("parent PK intact after name drop", 2, parentPKCount.intValue());
        assertEquals("group index intact after name drop", 6, bothValueCount.intValue());

        executeStatement.executeUpdate("DROP INDEX value");
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
