package com.akiban.cserver.store;

import java.io.File;
import java.util.ArrayList;
import java.util.PriorityQueue;

import junit.framework.TestCase;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.CServerConfig;
import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.RowDefCache;
import com.persistit.Key;
import com.persistit.KeyState;

public class VOrderTest extends TestCase implements CServerConstants {

    private final static String DDL_FILE_NAME = "src/test/resources/vordering_test.ddl";
    private final static String PREFIX = "vorder_test";
    private PersistitStore store;
    private RowDefCache rowDefCache;
    private ArrayList<KeyState> hkeysGG;
    private ArrayList<KeyState> hkeysG;
    private ArrayList<KeyState> hkeysP;
    private ArrayList<KeyState> hkeysC;

    @Override
    public void setUp() throws Exception {
        rowDefCache = new RowDefCache();
        store = new PersistitStore(CServerConfig.unitTestConfig(), rowDefCache);
        final AkibaInformationSchema ais = new DDLSource()
                .buildAIS(DDL_FILE_NAME);
        rowDefCache.setAIS(ais);
        store.startUp();
        store.setOrdinals();
        // find a user table.

        // great grand
        // 1
        // grand
        // 11 12
        // parent
        // 111 112
        // 121 122
        // child
        // 1111 1112
        // 1121 1122
        // 1211 1212
        // 1221 1222

        Key key1 = new Key(store.getDb());

        HKeyColumnArrayGenerator generator = new HKeyColumnArrayGenerator(
                PREFIX + "great_grand_parent");
        key1.clear();
        generator.append(new KeyState(key1), 1);
        hkeysGG = generator.getKeys();
        assert hkeysGG.size() == 1;

        generator = new HKeyColumnArrayGenerator(PREFIX + "grand_parent");
        generator.append(hkeysGG.get(0), 2);
        hkeysG = generator.getKeys();
        assert hkeysG.size() == 2;

        generator = new HKeyColumnArrayGenerator(PREFIX + "parent");
        generator.append(hkeysG.get(0), 2);
        generator.append(hkeysG.get(1), 2);
        hkeysP = generator.getKeys();
        assert hkeysP.size() == 4;

        generator = new HKeyColumnArrayGenerator(PREFIX + "child");
        generator.append(hkeysP.get(0), 2);
        generator.append(hkeysP.get(1), 2);
        generator.append(hkeysP.get(2), 2);
        generator.append(hkeysP.get(3), 2);
        hkeysC = generator.getKeys();
        assert hkeysC.size() == 8;
    }

    @Override
    public void tearDown() throws Exception {
        store.shutDown();
        store = null;
        rowDefCache = null;
    }

    public void testIt() throws Exception {
        VariableLengthArray gg = new VariableLengthArray(new File(PREFIX
                + "great_grand_parent-hkey.meta"), new File(PREFIX
                + "great_grand_parent-hkey.data"));
        VariableLengthArray g = new VariableLengthArray(new File(PREFIX
                + "grand_parent-hkey.meta"), new File(PREFIX
                + "grand_parent-hkey.data"));

        VariableLengthArray p = new VariableLengthArray(new File(PREFIX
                + "parent-hkey.meta"), new File(PREFIX + "parent-hkey.data"));

        VariableLengthArray c = new VariableLengthArray(new File(PREFIX
                + "child-hkey.meta"), new File(PREFIX + "child-hkey.data"));
        // byte[] bytes = new byte[1];
        // KeyState key = new KeyState(bytes);

        PriorityQueue<KeyState> pque = new PriorityQueue<KeyState>();
        boolean hasMore = true;
        while (hasMore) {
            int keySize = gg.getNextFieldSize();
            byte[] keyBytes = new byte[keySize];
            hasMore = gg.copyNextField(keyBytes, 0);
            assertTrue(pque.add(new KeyState(keyBytes)));
        }
        hasMore = true;
        while (hasMore) {
            int keySize = g.getNextFieldSize();
            byte[] keyBytes = new byte[keySize];
            hasMore = g.copyNextField(keyBytes, 0);
            assertTrue(pque.add(new KeyState(keyBytes)));
        }

        hasMore = true;
        while (hasMore) {
            int keySize = p.getNextFieldSize();
            byte[] keyBytes = new byte[keySize];
            hasMore = p.copyNextField(keyBytes, 0);
            assertTrue(pque.add(new KeyState(keyBytes)));
        }

        hasMore = true;
        while (hasMore) {
            int keySize = c.getNextFieldSize();
            byte[] keyBytes = new byte[keySize];
            hasMore = c.copyNextField(keyBytes, 0);
            assertTrue(pque.add(new KeyState(keyBytes)));
        }

        KeyState top = pque.poll();
        assertTrue(top.equals(hkeysGG.get(0)));
        assertTrue(pque.poll().equals(hkeysG.get(0)));
        assertTrue(pque.poll().equals(hkeysP.get(0)));
        assertTrue(pque.poll().equals(hkeysC.get(0)));
        assertTrue(pque.poll().equals(hkeysC.get(1)));
        assertTrue(pque.poll().equals(hkeysP.get(1)));
        assertTrue(pque.poll().equals(hkeysC.get(2)));
        assertTrue(pque.poll().equals(hkeysC.get(3)));
        assertTrue(pque.poll().equals(hkeysG.get(1)));
        assertTrue(pque.poll().equals(hkeysP.get(2)));
        assertTrue(pque.poll().equals(hkeysC.get(4)));
        assertTrue(pque.poll().equals(hkeysC.get(5)));
        assertTrue(pque.poll().equals(hkeysP.get(3)));
        assertTrue(pque.poll().equals(hkeysC.get(6)));
        assertTrue(pque.poll().equals(hkeysC.get(7)));
    }
}

/*
 * RowDef testRowDef=null; List<RowDef> rowDefs = rowDefCache.getRowDefs();
 * Iterator<RowDef> i = rowDefs.iterator(); while (i.hasNext()) { RowDef rowDef
 * = i.next(); if (!rowDef.isGroupTable()) { continue; }
 * 
 * assert(rowDef.getTableName().equals("_akiba_htest"));
 * System.out.println("name = "
 * +rowDef.getTableName()+", id = "+rowDef.getRowDefId()); testRowDef = rowDef;
 * break; }
 * 
 * assert testRowDef != null;
 * 
 * RowDef[] tables = testRowDef.getUserTableRowDefs(); //RowDef table = null;
 * ArrayList<RowDef> order = new ArrayList<RowDef>(); int depth = 2; for(int j =
 * 0; j < tables.length; j++) {
 * 
 * if(tables[j].getHKeyDepth() == depth) { System.out.print("name = "
 * +tables[j].getTableName()+"tables getHkeydepth = "+tables[j].getHKeyDepth());
 * System.out.println("Depth found"); order.add(tables[j]); j = -1; depth += 2;
 * } }
 * 
 * Iterator<RowDef> tableIter = order.iterator(); RowDef table =
 * tableIter.next(); assert table.getPkFields().length == 1; //FieldDef field =
 * table.getFieldDef(table.getPkFields()[0]); //Encoding coder =
 * Encoding.valueOf(Types.INT.encoding()); //assert coder.validate(Types.INT);
 * 
 * Key key1 = new Key(store.getDb()); key1.append(1); Tree<Key> tree1 = new
 * Tree<Key>(key1); coder.toKey(field, new Integer(1), key1);
 * 
 * Key key11 = new Key(store.getDb()); coder.toKey(field, new Integer(11),
 * key11); Tree<Key> tree11 = new Tree<Key>(key11); tree1.add(tree11);
 * 
 * Key key12 = new Key(store.getDb()); coder.toKey(field, new Integer(12),
 * key12); Tree<Key> tree12 = new Tree<Key>(key12); tree1.add(tree12);
 * 
 * Key key111 = new Key(store.getDb()); coder.toKey(field, new Integer(111),
 * key11); Tree<Key> tree111 = new Tree<Key>(key111); tree11.add(tree111);
 * 
 * Key key112 = new Key(store.getDb()); coder.toKey(field, new Integer(112),
 * key112); Tree<Key> tree112 = new Tree<Key>(key112); tree11.add(tree112);
 * 
 * Key key121 = new Key(store.getDb()); coder.toKey(field, new Integer(121),
 * key121); Tree<Key> tree121 = new Tree<Key>(key121); tree12.add(tree121);
 * 
 * Key key122 = new Key(store.getDb()); coder.toKey(field, new Integer(122),
 * key122); Tree<Key> tree122 = new Tree<Key>(key122); tree12.add(tree122);
 * 
 * Key key1111 = new Key(store.getDb()); coder.toKey(field, new Integer(1111),
 * key1111); Tree<Key> tree1111 = new Tree<Key>(key1111); tree111.add(tree1111);
 * 
 * Key key1112 = new Key(store.getDb()); coder.toKey(field, new Integer(1112),
 * key1112); Tree<Key> tree1112 = new Tree<Key>(key1112); tree111.add(tree1112);
 * 
 * Key key1121 = new Key(store.getDb()); coder.toKey(field, new Integer(1121),
 * key1121); Tree<Key> tree1121 = new Tree<Key>(key1121); tree112.add(tree1121);
 * 
 * Key key1122 = new Key(store.getDb()); coder.toKey(field, new Integer(1122),
 * key1122); Tree<Key> tree1122 = new Tree<Key>(key1122); tree112.add(tree1122);
 * 
 * Key key1211 = new Key(store.getDb()); coder.toKey(field, new Integer(1211),
 * key1211); Tree<Key> tree1211 = new Tree<Key>(key1211); tree121.add(tree1211);
 * 
 * Key key1212 = new Key(store.getDb()); coder.toKey(field, new Integer(1212),
 * key1212); Tree<Key> tree1212 = new Tree<Key>(key1212); tree121.add(tree1212);
 * 
 * Key key1221 = new Key(store.getDb()); coder.toKey(field, new Integer(1221),
 * key1221); Tree<Key> tree1221 = new Tree<Key>(key1221); tree122.add(tree1221);
 * 
 * Key key1222 = new Key(store.getDb()); coder.toKey(field, new Integer(1222),
 * key1222); Tree<Key> tree1222 = new Tree<Key>(key1222); tree122.add(tree1222);
 */

