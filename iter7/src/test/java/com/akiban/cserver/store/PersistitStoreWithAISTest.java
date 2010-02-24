package com.akiban.cserver.store;

import java.io.File;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.CServerConfig;
import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.util.ByteBufferFactory;
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
		final RowDef defA = rowDefCache.getRowDef("address");
		final RowDef defX = rowDefCache.getRowDef("component");
		final RowDef defCOI = rowDefCache.getRowDef("_akiba_coi");
		final RowData rowC = new RowData(new byte[64]);
		final RowData rowO = new RowData(new byte[64]);
		final RowData rowI = new RowData(new byte[64]);
		final RowData rowA = new RowData(new byte[64]);
		final RowData rowX = new RowData(new byte[64]);
		final RowData rowCOI = new RowData(new byte[1024]);
		final int customers;
		final int ordersPerCustomer;
		final int itemsPerOrder;
		final int componentsPerItem;

		long elapsed;

		TestData(final int customers, final int ordersPerCustomer,
				final int itemsPerOrder, final int componentsPerItem) {
			this.customers = customers;
			this.ordersPerCustomer = ordersPerCustomer;
			this.itemsPerOrder = itemsPerOrder;
			this.componentsPerItem = componentsPerItem;
		}

		void insertTestRows() throws Exception {
			elapsed = System.nanoTime();
			int unique = 0;
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
						for (int x = 0; ++x <= 5;) {
							int xid = iid * 1000 + x;
							rowX.reset(0, 64);
							rowX.createRow(defX, new Object[] { iid, xid, c,
									++unique });
							assertEquals(OK, store.writeRow(rowX));
						}
					}
				}
				for (int a = 0; a < (c % 3); a++) {
					rowA.reset(0, 64);
					rowA.createRow(defA, new Object[] { c, a, "addr1_" + c,
							"addr2_" + c, "addr3_" + c });
					assertEquals(OK, store.writeRow(rowA));
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
		
		void start() {
			elapsed = System.nanoTime();
		}
		
		void end() {
			elapsed = System.nanoTime() - elapsed;
		}
	}

	@Override
	public void setUp() throws Exception {
		rowDefCache = new RowDefCache();
		store = new PersistitStore(new CServerConfig(), rowDefCache);
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
		final TestData td = new TestData(10, 10, 10, 10);
		td.insertTestRows();
		System.out.println("testWriteCOIrows: inserted " + td.totalRows() + " rows in "
				+ (td.elapsed / 1000L) + "us");

	}

	public void testScanCOIrows() throws Exception {
		TestData td = new TestData(1000, 10, 3, 2);
		td.insertTestRows();
		long t2 = System.nanoTime();
		System.out.println("testScanCOIrows: inserted " + td.totalRows() + " rows in "
				+ (td.elapsed / 1000L) + "us");
		{
			// simple test - get all I rows
			td.start();
			int scanCount = 0;
			td.rowI.createRow(td.defI, new Object[] { null, null, null });
			final byte[] columnBitMap = new byte[] { (byte) 0xFF, (byte) 0xFF,
					(byte) 0xFF, (byte) 0xFF };
			final RowCollector rc = store.newRowCollector(1111, td.rowI,
					td.rowI, columnBitMap);
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
			td.end();
			System.out.println("testScanCOIrows: scanned " + scanCount + " rows in "
					+ (td.elapsed / 1000L) + "us");
			
		}

		{
			// select item by IID in user table `item`
			td.start();
			int scanCount = 0;
			td.rowI.createRow(td.defI, new Object[] { null,
					Integer.valueOf(1001001), null, null });
			final byte[] columnBitMap = new byte[] { (byte) 0x3 };
			final RowCollector rc = store.newRowCollector(1111, td.rowI,
					td.rowI, columnBitMap);
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
			td.end();
			System.out.println("testScanCOIrows: scanned " + scanCount + " rows in "
					+ (td.elapsed / 1000L) + "us");

		}

		{
			// select items in COI table by index values on Order
			td.start();
			int scanCount = 0;
			final RowData start = new RowData(new byte[256]);
			final RowData end = new RowData(new byte[256]);
			// C has 2 columns, A has 5 columns, O has 3 columns, I has 4
			// columns, CC has 5 columns
			start.createRow(td.defCOI, new Object[] { null, null, null, null,
					null, null, null, 1004, null, null, null, null, null, null,
					null, null, null, null, null });
			end.createRow(td.defCOI, new Object[] { null, null, null, null,
					null, null, null, 1007, null, null, null, null, null, null,
					null, null, null, null, null });
			final byte[] columnBitMap = new byte[] { (byte) 0x83, (byte) 0x3F,
					(byte) 0 };
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
			td.end();
			System.out.println("testScanCOIrows: scanned " + scanCount + " rows in "
					+ (td.elapsed / 1000L) + "us");
		}
	}

	public void testDropTable() throws Exception {
		TestData td = new TestData(10, 10, 10, 10);
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

	public void testUniqueIndexes() throws Exception {
		TestData td = new TestData(10, 10, 10, 10);
		td.insertTestRows();
		td.rowX.createRow(td.defX, new Object[]{37906, 23890345, 123, 44, "test1" });
		int result;
		result = store.writeRow(td.rowX);
		assertEquals(HA_ERR_FOUND_DUPP_KEY, result);
		td.rowX.createRow(td.defX, new Object[]{37906, 23890345, 123, 44444, "test2" });
		result = store.writeRow(td.rowX);
		assertEquals(OK, result);
		
	}

}
