package com.akiba.cserver.store;

import java.io.File;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import com.akiba.ais.ddl.DDLSource;
import com.akiba.ais.model.AkibaInformationSchema;
import com.akiba.cserver.CServerConstants;
import com.akiba.cserver.CServerUtil;
import com.akiba.cserver.RowData;
import com.akiba.cserver.RowDef;
import com.akiba.cserver.RowDefCache;
import com.akiba.util.ByteBufferFactory;

public class PersistitStoreWithAISTest extends TestCase implements
		CServerConstants {

	private final static File DATA_PATH = new File("/tmp/data");

	private final static String DDL_FILE_NAME = "src/test/resources/data_dictionary_test.ddl";

	private PersistitStore store;

	private RowDefCache rowDefCache;

	@Override
	public void setUp() throws Exception {
		rowDefCache = new RowDefCache();
		store = new PersistitStore(rowDefCache);
		CServerUtil.cleanUpDirectory(DATA_PATH);
		PersistitStore.setDataPath(DATA_PATH.getPath());
		final AkibaInformationSchema ais = new DDLSource()
				.buildAIS(DDL_FILE_NAME);
		rowDefCache.setAIS(ais);
		store.startUp();
	}

	@Override
	public void tearDown() throws Exception {
		store.shutDown();
		store = null;
		rowDefCache = null;
	}

	public void testWriteCOIrows() throws Exception {
		final RowDef defC = rowDefCache.getRowDef("customer");
		final RowDef defO = rowDefCache.getRowDef("order");
		final RowDef defI = rowDefCache.getRowDef("item");
		final RowData rowC = new RowData(new byte[64]);
		final RowData rowO = new RowData(new byte[64]);
		final RowData rowI = new RowData(new byte[64]);

		final long start = System.nanoTime();
		int count = 0;
		for (int c = 0; ++c <= 1000;) {
			int cid = c;
			rowC.reset(0, 64);
			rowC.createRow(defC, new Object[] { cid, "Customer_" + cid });
			assertEquals(OK, store.writeRow(rowC));
			count++;
			for (int o = 0; ++o <= 10;) {
				int oid = cid * 1000 + o;
				rowO.reset(0, 64);
				rowO.createRow(defO, new Object[] { oid, cid, 12345 });
				assertEquals(OK, store.writeRow(rowO));
				count++;
				for (int i = 0; ++i <= 3;) {
					int iid = oid * 1000 + i;
					rowI.reset(0, 64);
					rowI.createRow(defI, new Object[] { oid, iid, 123456,
							654321 });
					assertEquals(OK, store.writeRow(rowI));
					count++;
				}
			}
		}
		final long elapsed = System.nanoTime() - start;
		System.out.println("Inserting " + count + " rows in "
				+ (elapsed / 1000000L) + "ms");

	}

	public void testScanCOIrows() throws Exception {
		final RowDef defC = rowDefCache.getRowDef("customer");
		final RowDef defO = rowDefCache.getRowDef("order");
		final RowDef defI = rowDefCache.getRowDef("item");
		final RowData rowC = new RowData(new byte[64]);
		final RowData rowO = new RowData(new byte[64]);
		final RowData rowI = new RowData(new byte[64]);
		int insertCount = 0;

		// Note: we are going to scan for I rows, so we'll only
		// count inserts of I rows.
		for (int c = 0; ++c <= 10;) {
			int cid = c;
			rowC.reset(0, 64);
			rowC.createRow(defC, new Object[] { cid, "Customer_" + cid });
			assertEquals(OK, store.writeRow(rowC));
			for (int o = 0; ++o <= 10;) {
				int oid = cid * 1000 + o;
				rowO.reset(0, 64);
				rowO.createRow(defO, new Object[] { oid, cid, 12345 });
				assertEquals(OK, store.writeRow(rowO));
				for (int i = 0; ++i <= 10;) {
					int iid = oid * 1000 + i;
					rowI.reset(0, 64);
					rowI.createRow(defI, new Object[] { oid, iid, 123456,
							654321 });
					assertEquals(OK, store.writeRow(rowI));
					insertCount++;
				}
			}
		}

		{
			// simple test - get all I rows
			int scanCount = 0;
			rowI.createRow(defI, new Object[] { null, null, null });
			final byte[] columnBitMap = new byte[] { (byte) 0xFF, (byte) 0xFF,
					(byte) 0xFF, (byte) 0xFF };
			final RowCollector rc = store.newRowCollector(1111, rowI, rowI,
					columnBitMap);
			final ByteBuffer payload = ByteBufferFactory.allocate(256);

			while (rc.hasMore()) {
				payload.clear();
				while (rc.collectNextRow(payload))
					;
				payload.flip();
				RowData rowData = new RowData(payload.array(), payload
						.position(), payload.limit());
				for (int p = rowData.getBufferStart(); p < rowData
						.getBufferEnd();) {
					rowData.prepareRow(p);
					p = rowData.getRowEnd();
					scanCount++;
				}
			}
			assertEquals(insertCount, scanCount);
		}

		{
			// select item by IID in user table `item`
			int scanCount = 0;
			rowI.createRow(defI, new Object[] { null, Integer.valueOf(1001001),
					null, null });
			final byte[] columnBitMap = new byte[] { (byte) 0x3 };
			final RowCollector rc = store.newRowCollector(1111, rowI, rowI,
					columnBitMap);
			final ByteBuffer payload = ByteBufferFactory.allocate(256);

			while (rc.hasMore()) {
				payload.clear();
				while (rc.collectNextRow(payload))
					;
				payload.flip();
				RowData rowData = new RowData(payload.array(), payload
						.position(), payload.limit());
				for (int p = rowData.getBufferStart(); p < rowData
						.getBufferEnd();) {
					rowData.prepareRow(p);
					p = rowData.getRowEnd();
					scanCount++;
				}
			}
			assertEquals(1, scanCount);
		}

		{
			// select items in COI table by index values on Order
			int scanCount = 0;
			final RowDef defCOI = rowDefCache.getRowDef("_akiba_coi");
			final RowData start = new RowData(new byte[256]);
			final RowData end = new RowData(new byte[256]);
			// C has 2 columns, O has 3 columns, I has 4 columns
			start.createRow(defCOI, new Object[] { null, null, 1004, null,
					null, null, null, null, null });
			end.createRow(defCOI, new Object[] { null, null, 1007, null, null,
					null, null, null, null });
			final byte[] columnBitMap = new byte[] { (byte) 0xFF, (byte) 1 };
			final RowCollector rc = store.newRowCollector(1111, start, end,
					columnBitMap);
			final ByteBuffer payload = ByteBufferFactory.allocate(256);
			//
			// Expect all the C, O and I rows for orders 1004 through 1007, inclusive
			// Total of 40
			//
			while (rc.hasMore()) {
				payload.clear();
				while (rc.collectNextRow(payload))
					;
				payload.flip();
				RowData rowData = new RowData(payload.array(), payload
						.position(), payload.limit());
				for (int p = rowData.getBufferStart(); p < rowData
						.getBufferEnd();) {
					rowData.prepareRow(p);
					p = rowData.getRowEnd();
					scanCount++;
				}
			}
			assertEquals(45, scanCount);
		}

	}

}
