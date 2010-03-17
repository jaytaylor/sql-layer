package com.akiban.cserver.store;

import java.io.File;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.CServerConfig;
import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.IndexDef;
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

	private interface RowVisitor {
		void visit(final int depth) throws Exception;
	}

	private class TestData {
		final RowDef defC = rowDefCache.getRowDef("customer");
		final RowDef defO = rowDefCache.getRowDef("order");
		final RowDef defI = rowDefCache.getRowDef("item");
		final RowDef defA = rowDefCache.getRowDef("address");
		final RowDef defX = rowDefCache.getRowDef("component");
		final RowDef defCOI = rowDefCache.getRowDef("_akiba_coi");
		final RowData rowC = new RowData(new byte[256]);
		final RowData rowO = new RowData(new byte[256]);
		final RowData rowI = new RowData(new byte[256]);
		final RowData rowA = new RowData(new byte[256]);
		final RowData rowX = new RowData(new byte[256]);
		final RowData rowCOI = new RowData(new byte[1024]);
		final int customers;
		final int ordersPerCustomer;
		final int itemsPerOrder;
		final int componentsPerItem;

		long cid;
		long oid;
		long iid;
		long xid;

		long elapsed;
		long count = 0;

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
				cid = c;
				rowC.reset(0, 256);
				rowC.createRow(defC, new Object[] { cid, "Customer_" + cid });
				assertEquals(OK, store.writeRow(rowC));
				for (int o = 0; ++o <= ordersPerCustomer;) {
					oid = cid * 1000 + o;
					rowO.reset(0, 256);
					rowO.createRow(defO, new Object[] { oid, cid, 12345 });
					assertEquals(OK, store.writeRow(rowO));
					for (int i = 0; ++i <= itemsPerOrder;) {
						iid = oid * 1000 + i;
						rowI.reset(0, 256);
						rowI.createRow(defI, new Object[] { oid, iid, 123456,
								654321 });
						assertEquals(OK, store.writeRow(rowI));
						for (int x = 0; ++x <= componentsPerItem;) {
							xid = iid * 1000 + x;
							rowX.reset(0, 256);
							rowX.createRow(defX, new Object[] { iid, xid, c,
									++unique, "Description_" + unique });
							assertEquals(OK, store.writeRow(rowX));
						}
					}
				}
				for (int a = 0; a < (c % 3); a++) {
					rowA.reset(0, 256);
					rowA.createRow(defA, new Object[] { c, a, "addr1_" + c,
							"addr2_" + c, "addr3_" + c });
					assertEquals(OK, store.writeRow(rowA));
				}
			}
			elapsed = System.nanoTime() - elapsed;
		}

		void visitTestRows(final RowVisitor visitor) throws Exception {
			elapsed = System.nanoTime();
			int unique = 0;
			for (int c = 0; ++c <= customers;) {
				cid = c;
				rowC.reset(0, 256);
				rowC.createRow(defC, new Object[] { cid, "Customer_" + cid });
				visitor.visit(0);
				for (int o = 0; ++o <= ordersPerCustomer;) {
					oid = cid * 1000 + o;
					rowO.reset(0, 256);
					rowO.createRow(defO, new Object[] { oid, cid, 12345 });
					visitor.visit(1);
					for (int i = 0; ++i <= itemsPerOrder;) {
						iid = oid * 1000 + i;
						rowI.reset(0, 256);
						rowI.createRow(defI, new Object[] { oid, iid, 123456,
								654321 });
						visitor.visit(2);
						for (int x = 0; ++x <= componentsPerItem;) {
							xid = iid * 1000 + x;
							rowX.reset(0, 256);
							rowX.createRow(defX, new Object[] { iid, xid, c,
									++unique, "Description_" + unique });
							visitor.visit(3);
						}
					}
				}
			}
			elapsed = System.nanoTime() - elapsed;

		}

		int totalRows() {
			return totalCustomerRows() + totalOrderRows() + totalItemRows()
					+ totalComponentRows();
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

		int totalComponentRows() {
			return customers * ordersPerCustomer * itemsPerOrder
					* componentsPerItem;
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
		System.out.println("testWriteCOIrows: inserted " + td.totalRows()
				+ " rows in " + (td.elapsed / 1000L) + "us");

	}

	public void testScanCOIrows() throws Exception {
		final TestData td = new TestData(1000, 10, 3, 2);
		td.insertTestRows();
		System.out.println("testScanCOIrows: inserted " + td.totalRows()
				+ " rows in " + (td.elapsed / 1000L) + "us");
		{
			// simple test - get all I rows
			td.start();
			int scanCount = 0;
			td.rowI.createRow(td.defI, new Object[] { null, null, null });

			final byte[] columnBitMap = new byte[] { 0xF };
			final int indexId = 0;

			final RowCollector rc = store.newRowCollector(indexId, td.rowI,
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
			System.out.println("testScanCOIrows: scanned " + scanCount
					+ " rows in " + (td.elapsed / 1000L) + "us");

		}

		{
			// select item by IID in user table `item`
			td.start();
			int scanCount = 0;
			td.rowI.createRow(td.defI, new Object[] { null,
					Integer.valueOf(1001001), null, null });

			final byte[] columnBitMap = new byte[] { (byte) 0x3 };
			final int indexId = td.defI.getPKIndexDef().getId();

			final RowCollector rc = store.newRowCollector(indexId, td.rowI,
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
			System.out.println("testScanCOIrows: scanned " + scanCount
					+ " rows in " + (td.elapsed / 1000L) + "us");

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
			final byte[] columnBitMap = projection(new RowDef[] { td.defC,
					td.defO, td.defI }, td.defCOI.getFieldCount());

			int indexId = findIndexId(td.defCOI, td.defO, 0);
			final RowCollector rc = store.newRowCollector(indexId, start, end,
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
			System.out.println("testScanCOIrows: scanned " + scanCount
					+ " rows in " + (td.elapsed / 1000L) + "us");
		}
	}
	
	int findIndexId(final RowDef groupRowDef, final RowDef userRowDef, final int fieldIndex) {
		int indexId = -1;
		final int findField = fieldIndex + userRowDef.getColumnOffset();
		for (final IndexDef indexDef : groupRowDef.getIndexDefs()) {
			if (indexDef.getFields().length == 1
					&& indexDef.getFields()[0] == findField) {
				indexId = indexDef.getId();
			}
		}
		return indexId;
	}

	final byte[] projection(final RowDef[] rowDefs, final int width) {
		final byte[] bitMap = new byte[(width + 7) / 8];
		for (final RowDef rowDef : rowDefs) {
			for (int bit = rowDef.getColumnOffset(); bit < rowDef
					.getColumnOffset()
					+ rowDef.getFieldCount(); bit++) {
				bitMap[bit / 8] |= (1 << (bit % 8));
			}
		}
		return bitMap;
	}

	public void testDropTable() throws Exception {
		final TestData td = new TestData(5, 5, 5, 5);
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
		final TestData td = new TestData(5, 5, 5, 5);
		td.insertTestRows();
		td.rowX.createRow(td.defX, new Object[] { 1002003, 23890345, 123, 44,
				"test1" });
		int result;
		result = store.writeRow(td.rowX);
		assertEquals(HA_ERR_FOUND_DUPP_KEY, result);
		td.rowX.createRow(td.defX, new Object[] { 1002003, 23890345, 123,
				44444, "test2" });
		result = store.writeRow(td.rowX);
		assertEquals(OK, result);
	}

	public void testUpdateRows() throws Exception {
		final TestData td = new TestData(5, 5, 5, 5);
		td.insertTestRows();
		long cid = 3;
		long oid = cid * 1000 + 2;
		long iid = oid * 1000 + 4;
		long xid = iid * 1000 + 3;
		td.rowX.createRow(td.defX, new Object[] { iid, xid, null, null });
		final byte[] columnBitMap = new byte[] { (byte) 0x1F };
		final ByteBuffer payload = ByteBufferFactory.allocate(1024);

		RowCollector rc;
		rc = store.newRowCollector(td.defX.getPKIndexDef().getId(), td.rowX,
				td.rowX, columnBitMap);
		payload.clear();
		assertTrue(rc.collectNextRow(payload));
		payload.flip();
		RowData oldRowData = new RowData(payload.array(), payload.position(),
				payload.limit());
		oldRowData.prepareRow(oldRowData.getBufferStart());

		RowData newRowData = new RowData(new byte[256]);
		newRowData.createRow(td.defX, new Object[] { iid, xid, 4, 424242,
				"Description_424242" });
		store.updateRow(oldRowData, newRowData);

		rc = store.newRowCollector(td.defX.getPKIndexDef().getId(), td.rowX,
				td.rowX, columnBitMap);
		payload.clear();
		assertTrue(rc.collectNextRow(payload));
		payload.flip();

		RowData updateRowData = new RowData(payload.array(),
				payload.position(), payload.limit());
		updateRowData.prepareRow(updateRowData.getBufferStart());
		System.out.println(updateRowData.toString(store.getRowDefCache()));
		// TODO:
		// Hand-checked the index tables. Need SELECT on secondary indexes to
		// verify them automatically.
	}

	public void testDeleteRows() throws Exception {
		final TestData td = new TestData(5, 5, 5, 5);
		td.insertTestRows();
		td.count = 0;
		final RowVisitor visitor = new RowVisitor() {
			public void visit(final int depth) throws Exception {
				switch (depth) {
				case 0:
					// TODO - for now we can't do cascading DELETE so we
					// expect an error
					assertNotSame(OK, store.deleteRow(td.rowC));
					break;
				case 1:
					// TODO - for now we can't do cascading DELETE so we
					// expect an error
					assertNotSame(OK, store.deleteRow(td.rowO));
					break;
				case 2:
					// TODO - for now we can't do cascading DELETE so we
					// expect an error
					assertNotSame(OK, store.deleteRow(td.rowI));
					break;
				case 3:
					if (td.xid % 2 == 0) {
						assertEquals(OK, store.deleteRow(td.rowX));
						td.count++;
					}
				}
			}
		};
		td.visitTestRows(visitor);

		int scanCount = 0;
		td.rowX.createRow(td.defX, new Object[0]);
		final byte[] columnBitMap = new byte[] { (byte) 0x1F };
		final RowCollector rc = store.newRowCollector(td.defX.getPKIndexDef()
				.getId(), td.rowX, td.rowX, columnBitMap);
		final ByteBuffer payload = ByteBufferFactory.allocate(256);

		while (rc.hasMore()) {
			payload.clear();
			while (rc.collectNextRow(payload))
				;
			payload.flip();
			RowData rowData = new RowData(payload.array(), payload.position(),
					payload.limit());
			for (int p = rowData.getBufferStart(); p < rowData.getBufferEnd();) {
				rowData.prepareRow(p);
				p = rowData.getRowEnd();
				scanCount++;
			}
		}
		assertEquals(td.totalComponentRows() - td.count, scanCount);
		// TODO:
		// Hand-checked the index tables. Need SELECT on secondary indexes to
		// verify them automatically.
	}

}
