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

package com.akiban.server.store.statistics;

import com.akiban.sql.pg.PostgresServerFilesITBase;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;

import org.junit.After;
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
                 + IndexStatisticsLifecycleIT.class.getPackage().getName().replace('.', '/'));

    @Before
    public void loadDatabase() throws Exception {
        loadDatabase(RESOURCE_DIR);
    }

    protected Statement executeStatement;
    protected PreparedStatement checkStatement;
    protected final String CHECK_SQL = "SELECT header.table_id, header.index_id, COUNT(detail.column_count) AS ndetail FROM "+
            IndexStatisticsService.INDEX_STATISTICS_TABLE_NAME.getDescription() + " header LEFT JOIN " +
            IndexStatisticsService.INDEX_STATISTICS_ENTRY_TABLE_NAME.getDescription() + " detail USING (table_id, index_id) GROUP BY header.table_id, header.index_id";

    @Before
    public void prepareStatements() throws Exception {
        executeStatement = getConnection().createStatement();
        checkStatement = getConnection().prepareStatement(CHECK_SQL);
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
        GroupIndex bothValue = ais.getGroup("parent").getIndex("value");
        Integer bothValueCount = entries.get(bothValue);
        assertNotNull("group index was analyzed", bothValueCount);
        assertEquals("group index two entries", 4, bothValueCount.intValue());

        executeStatement.executeUpdate("DROP INDEX parent.name");
        entries = check();
        ais = ddl().getAIS(session());
        parentPK = ais.getTable(SCHEMA_NAME, "parent").getIndex("PRIMARY");
        bothValue = ais.getGroup("parent").getIndex("value");
        parentPKCount = entries.get(parentPK);
        bothValueCount = entries.get(bothValue);
        assertEquals("parent PK intact after name drop", 2, parentPKCount.intValue());
        assertEquals("group index intact after name drop", 4, bothValueCount.intValue());

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
