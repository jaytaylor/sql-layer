package com.akiba.cserver.store;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import com.akiba.ais.ddl.DDLSource;
import com.akiba.ais.model.AkibaInformationSchema;
import com.akiba.cserver.CServerConstants;
import com.akiba.cserver.CServerUtil;
import com.akiba.cserver.RowData;
import com.akiba.cserver.RowDef;
import com.akiba.cserver.RowDefCache;
import com.akiba.util.ByteBufferFactory;
import com.persistit.Volume;

public class PersistitStoreWithAISTest extends TestCase implements
		CServerConstants {

	private final static File DATA_PATH = new File("/tmp/data");

	private final static String DDL_FILE_NAME = "src/test/resources/data_dictionary_test.ddl";

	private PersistitStore store;

	private RowDefCache rowDefCache;

	private class TestData {
		final RowDef defC = rowDefCache.getRowDef("customer");
		final RowDef defO = rowDefCache.getRowDef("order");
		final RowDef defI = rowDefCache.getRowDef("item");
		final RowDef defCOI = rowDefCache.getRowDef("_akiba_coi");
		final RowData rowC = new RowData(new byte[64]);
		final RowData rowO = new RowData(new byte[64]);
		final RowData rowI = new RowData(new byte[64]);
		final RowData rowCOI = new RowData(new byte[1024]);
		final int customers;
		final int ordersPerCustomer;
		final int itemsPerOrder;

		long elapsed;

		TestData(final int customers, final int ordersPerCustomer,
				final int itemsPerOrder) {
			this.customers = customers;
			this.ordersPerCustomer = ordersPerCustomer;
			this.itemsPerOrder = itemsPerOrder;
		}

		void insertTestRows() throws Exception {
			elapsed = System.nanoTime();
			int count = 0;
			for (int c = 0; ++c <= customers;) {
				int cid = c;
				rowC.reset(0, 64);
				rowC.createRow(defC, new Object[] { cid, "Customer_" + cid });
				assertEquals(OK, store.writeRow(rowC));
				for (int o = 0; ++o <= ordersPerCustomer;) {
					int oid = cid * 1000 + o;
					rowO.reset(0, 64);
					rowO.createRow(defO, new Object[] { oid, cid, 12345 });
					assertEquals(OK, store.writeRow(rowO));
					for (int i = 0; ++i <= itemsPerOrder;) {
						int iid = oid * 1000 + i;
						rowI.reset(0, 64);
						rowI.createRow(defI, new Object[] { oid, iid, 123456,
								654321 });
						assertEquals(OK, store.writeRow(rowI));
					}
				}
			}
			elapsed = System.nanoTime() - elapsed;
		}

		int totalRows() {
			return customers + customers * ordersPerCustomer + customers
					* ordersPerCustomer * itemsPerOrder;
		}
		
		int totalCustomerRows() {
			return customers;
		}
		
		int totalOrderRows() {
			return customers * ordersPerCustomer;
		}
		
		int totalItemRows() {
			return customers * ordersPerCustomer * itemsPerOrder;
		}
	}

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
		final TestData td = new TestData(1000, 10, 3);
		td.insertTestRows();
		System.out.println("Inserting " + td.totalRows() + " rows in "
				+ (td.elapsed / 1000000L) + "ms");

	}

	public void testScanCOIrows() throws Exception {
		TestData td = new TestData(1000, 10, 3);
		td.insertTestRows();

		{
			// simple test - get all I rows
			int scanCount = 0;
			td.rowI.createRow(td.defI, new Object[] { null, null, null });
			final byte[] columnBitMap = new byte[] { (byte) 0xFF, (byte) 0xFF,
					(byte) 0xFF, (byte) 0xFF };
			final RowCollector rc = store.newRowCollector(1111, td.rowI, td.rowI,
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
			assertEquals(td.totalItemRows(), scanCount);
		}

		{
			// select item by IID in user table `item`
			int scanCount = 0;
			td.rowI.createRow(td.defI, new Object[] { null, Integer.valueOf(1001001),
					null, null });
			final byte[] columnBitMap = new byte[] { (byte) 0x3 };
			final RowCollector rc = store.newRowCollector(1111, td.rowI, td.rowI,
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
			final RowData start = new RowData(new byte[256]);
			final RowData end = new RowData(new byte[256]);
			// C has 2 columns, O has 3 columns, I has 4 columns
			start.createRow(td.defCOI, new Object[] { null, null, 1004, null,
					null, null, null, null, null });
			end.createRow(td.defCOI, new Object[] { null, null, 1007, null, null,
					null, null, null, null });
			final byte[] columnBitMap = new byte[] { (byte) 0xFF, (byte) 1 };
			final RowCollector rc = store.newRowCollector(1111, start, end,
					columnBitMap);
			final ByteBuffer payload = ByteBufferFactory.allocate(256);
			//
			// Expect all the C, O and I rows for orders 1004 through 1007,
			// inclusive
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
			assertEquals(17, scanCount);
		}
	}

	public void testDropTable() throws Exception {
		TestData td = new TestData(1000, 10, 3);
		td.insertTestRows();
		Volume volume = store.getDb().getVolume(PersistitStore.VOLUME_NAME);
		assertNotNull(volume.getTree(td.defCOI.getTreeName(), false));
		assertNotNull(volume.getTree(td.defO.getPkTreeName(), false));
		assertNotNull(volume.getTree(td.defI.getPkTreeName(), false));
		try {
			store.dropTable(td.defC.getRowDefId());
			fail("Should have thrown an Exception");
		} catch (Exception e) {
			// ok
		}
		int result = store.dropTable(td.defCOI.getRowDefId());
		assertEquals(OK, result);
		assertNull(volume.getTree(td.defCOI.getTreeName(), false));
		assertNull(volume.getTree(td.defO.getPkTreeName(), false));
		assertNull(volume.getTree(td.defI.getPkTreeName(), false));
	}

}
