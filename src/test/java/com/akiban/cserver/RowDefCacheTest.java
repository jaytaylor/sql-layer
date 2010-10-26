package com.akiban.cserver;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.store.PersistitStore;

/**
 * A preliminary unit test to verify HKey definition field in RowDefCache are
 * set up as specified in the schema.
 * 
 * @author peter
 * 
 */
public class RowDefCacheTest {

    protected final static String DDL_FILE_NAME = "src/test/resources/row_def_cache_test.ddl";

    protected final static String SCHEMA = "row_def_cache_test";

    protected static RowDefCache rowDefCache;

    @Before
    public void setUp() throws Exception {

        rowDefCache = new RowDefCache();
    }


    @Test
    public void testHKeyOrdering() throws Exception {
        final AkibaInformationSchema ais = new DDLSource()
                .buildAIS(DDL_FILE_NAME);
        rowDefCache.setAIS(ais);
        final RowDef b = rowDefCache.getRowDef(SCHEMA + ".b");
        final RowDef bb = rowDefCache.getRowDef(SCHEMA + ".bb");

        assertArrayEquals(new int[] { 3, 2, 4, 1 }, b.getPkFields());
        assertArrayEquals(new int[] {}, b.getParentJoinFields());
        assertArrayEquals(new int[] { 5, 4 }, bb.getPkFields());
        assertArrayEquals(new int[] { 0, 2, 1, 3 }, bb
                .getParentJoinFields());
    }

    @Test
    public void childDoesNotContributeToHKey() throws Exception {
        final String ddl =
                "use `" + SCHEMA +"`;\n" +
                "create table parent (\n" +
                "   id int,\n" +
                "   primary key(id)\n" +
                ") engine = akibandb;\n" +
                "create table zebra (\n" +
                "   id int,\n" +
                "   primary key(id),\n" +
                "   constraint `__akiban_fk0` foreign key `akibafk` (id) references parent(id)\n" +
                ") engine = akibandb;";

        final AkibaInformationSchema ais = new DDLSource().buildAISFromString(ddl);
        rowDefCache.setAIS(ais);
        final RowDef parent = rowDefCache.getRowDef(SCHEMA + ".parent");
        final RowDef zebra = rowDefCache.getRowDef(SCHEMA + ".zebra");

        assertArrayEquals("parent PKs", new int[]{0}, parent.getPkFields());
        assertArrayEquals("parent joins", new int[] {}, parent.getParentJoinFields());
        assertArrayEquals("zebra PKs", new int[]{}, zebra.getPkFields());
        assertArrayEquals("zebra joins", new int[] {0}, zebra.getParentJoinFields());
    }
}
