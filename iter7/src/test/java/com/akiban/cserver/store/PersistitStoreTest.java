package com.akiban.cserver.store;

import java.io.File;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.FieldType;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.util.ByteBufferFactory;
import com.persistit.Key;
import com.persistit.Persistit;

public class PersistitStoreTest extends TestCase implements CServerConstants {

	private final static File DATA_PATH = new File("/tmp/data");

	private final static RowDef ROW_DEF0 = RowDef.createRowDef(1234,
			new FieldDef[] { new FieldDef("t1", FieldType.INT),
					new FieldDef("t2", FieldType.INT),
					new FieldDef("t3", FieldType.INT) }, "test",
			"test_group_table", new int[] { 0 });

	//
	// Highly abbreviate "COI" structure.
	//
	private final static RowDef ROW_DEF_C = RowDef.createRowDef(100,
			new FieldDef[] { new FieldDef("customer_id", FieldType.INT),
					new FieldDef("customer_x", FieldType.VARCHAR, 100) },
			"customers", "coi", new int[] { 0 });

	private final static RowDef ROW_DEF_O = RowDef.createRowDef(101,
			new FieldDef[] { new FieldDef("order_id", FieldType.INT),
					new FieldDef("date", FieldType.INT),
					new FieldDef("description", FieldType.VARCHAR, 100) },
			"orders", "coi", new int[] { 0 }, 100, new int[] { 1 });

	private final static RowDef ROW_DEF_I = RowDef.createRowDef(102,
			new FieldDef[] { new FieldDef("item_id", FieldType.INT),
					new FieldDef("qty", FieldType.INT),
					new FieldDef("description", FieldType.VARCHAR, 100) },
			"items", "coi", new int[] { 0 }, 101, new int[] { 1 });

	private PersistitStore store;

	private RowDefCache rowDefCache;

	@Override
	public void setUp() throws Exception {
		rowDefCache = new RowDefCache();
		store = new PersistitStore(rowDefCache);
		CServerUtil.cleanUpDirectory(DATA_PATH);
		PersistitStore.setDataPath(DATA_PATH.getPath());
		store.startUp();
	}

	@Override
	public void tearDown() throws Exception {
		store.shutDown();
		store = null;
		rowDefCache = null;
	}

	public void testWriteSimpleRootRow() throws Exception {
		rowDefCache.putRowDef(ROW_DEF0);
		final RowData rowData = new RowData(new byte[64]);
		rowData.createRow(ROW_DEF0, new Object[] { 1, 2, 3 });
		assertEquals(OK, store.writeRow(rowData));
	}

	public void testWriteCOIrows() throws Exception {
		rowDefCache.putRowDef(ROW_DEF_C);
		rowDefCache.putRowDef(ROW_DEF_O);
		rowDefCache.putRowDef(ROW_DEF_I);
		final RowData rowC = new RowData(new byte[64]);
		final RowData rowO = new RowData(new byte[64]);
		final RowData rowI = new RowData(new byte[64]);

		for (int c = 0; ++c <= 10;) {
			int cid = c;
			rowC.reset(0, 64);
			rowC.createRow(ROW_DEF_C, new Object[] { cid, "Customer_" + cid });
			assertEquals(OK, store.writeRow(rowC));
			for (int o = 0; ++o <= 10;) {
				int oid = cid * 1000 + o;
				rowO.reset(0, 64);
				rowO.createRow(ROW_DEF_O, new Object[] { oid, cid,
						"Order_" + oid });
				assertEquals(OK, store.writeRow(rowO));
				for (int i = 0; ++i <= 10;) {
					int iid = oid * 1000 + i;
					rowI.reset(0, 64);
					rowI.createRow(ROW_DEF_I, new Object[] { iid, oid,
							"Item_" + iid });
					assertEquals(OK, store.writeRow(rowI));
				}
			}
		}

	}
	
	public void testSimpleScanCOIrows() throws Exception {
		rowDefCache.putRowDef(ROW_DEF_C);
		rowDefCache.putRowDef(ROW_DEF_O);
		rowDefCache.putRowDef(ROW_DEF_I);
		final RowData rowC = new RowData(new byte[64]);
		final RowData rowO = new RowData(new byte[64]);
		final RowData rowI = new RowData(new byte[64]);
		int insertCount = 0;
		int scanCount = 0;

		// Note: we are going to scan for I rows, so we'll only
		// count inserts of I rows.
		for (int c = 0; ++c <= 10;) {
			int cid = c;
			rowC.reset(0, 64);
			rowC.createRow(ROW_DEF_C, new Object[] { cid, "Customer_" + cid });
			assertEquals(OK, store.writeRow(rowC));
			for (int o = 0; ++o <= 10;) {
				int oid = cid * 1000 + o;
				rowO.reset(0, 64);
				rowO.createRow(ROW_DEF_O, new Object[] { oid, cid,
						"Order_" + oid });
				assertEquals(OK, store.writeRow(rowO));
				for (int i = 0; ++i <= 10;) {
					int iid = oid * 1000 + i;
					rowI.reset(0, 64);
					rowI.createRow(ROW_DEF_I, new Object[] { iid, oid,
							"Item_" + iid });
					assertEquals(OK, store.writeRow(rowI));
					insertCount++;
				}
			}
		}
		
		rowI.createRow(ROW_DEF_I, new Object[] {null, null, null});
		final byte[] columnBitMap = new byte[]{(byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF};
		final RowCollector rc = store.newRowCollector(1111, rowI, rowI, columnBitMap);
		final ByteBuffer payload = ByteBufferFactory.allocate(256);

		while (rc.hasMore()) {
			payload.clear();
			while(rc.collectNextRow(payload));
			payload.flip();
			RowData rowData = new RowData(payload.array(), payload.position(), payload.limit());
			for (int p = rowData.getBufferStart(); p < rowData.getBufferEnd();) {
				rowData.prepareRow(p);
				p = rowData.getRowEnd();
				scanCount++;
			}
		}
		assertEquals(insertCount, scanCount);
	}

	public void testCopyAndRotateKey() throws Exception {
		final Persistit db = store.getDb();
		Key key1 = new Key(db);
		Key key2 = new Key(db);

		key1.append("abc").append("def").append(3).append(-12354.678d);
		store.copyAndRotate(key1, key2, 1);
		assertEquals("{\"def\",3,-12354.678,\"abc\"}", key2.toString());
		store.copyAndRotate(key1, key2, -1);
		assertEquals("{-12354.678,\"abc\",\"def\",3}", key2.toString());
	}

}
