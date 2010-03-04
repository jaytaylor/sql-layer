package com.akiban.cserver.store;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.TestCase;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.CServerConfig;
import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.IndexDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.util.ByteBufferFactory;

public class ScanRowsTest extends TestCase implements CServerConstants {

	private final static File DATA_PATH = new File("/tmp/data");

	private final static String DDL_FILE_NAME = "src/test/resources/scan_rows_test.ddl";

	private final static int[] TENS = new int[] { 1, 10, 100, 1000, 10000,
			100000, 1000000, 10000000 };

	private final static boolean VERBOSE = false;

	private PersistitStore store;

	private RowDefCache rowDefCache;

	private SortedMap<String, UserTable> tableMap = new TreeMap<String, UserTable>();

	@Override
	public void setUp() throws Exception {
		rowDefCache = new RowDefCache();
		store = new PersistitStore(new CServerConfig(), rowDefCache);
		CServerUtil.cleanUpDirectory(DATA_PATH);
		PersistitStore.setDataPath(DATA_PATH.getPath());
		final AkibaInformationSchema ais = new DDLSource()
				.buildAIS(DDL_FILE_NAME);
		rowDefCache.setAIS(ais);
		for (UserTable table : ais.getUserTables().values()) {
			tableMap.put(table.getName().getTableName(), table);
		}
		store.startUp();
	}

	@Override
	public void tearDown() throws Exception {
		store.shutDown();
		store = null;
		rowDefCache = null;
	}

	private void populateTables() throws Exception {
		final RowData rowData = new RowData(new byte[256]);
		// Create the tables in alphabetical order. Bacause of the
		// way the tables are defined, this also creates all parents before
		// their children.
		for (String name : tableMap.keySet()) {
			final RowDef rowDef = rowDefCache.getRowDef(name);
			final int level = name.length();
			int k = (int) Math.pow(10, level);
			for (int i = 0; i < k; i++) {
				rowData.createRow(rowDef, new Object[] { (i / 10), i, 7, 8 });
				assertEquals(OK, store.writeRow(rowData));
			}
		}
	}

	private int scanAllRows(final String test, final RowData start,
			final RowData end, final byte[] columnBitMap, final int indexId)
			throws Exception {
		int scanCount = 0;
		final RowCollector rc = store.newRowCollector(indexId, start, end,
				columnBitMap);
		final ByteBuffer payload = ByteBufferFactory.allocate(256);
		if (VERBOSE) {
			System.out.println("Test " + test);
		}
		while (rc.hasMore()) {
			payload.clear();
			while (rc.collectNextRow(payload))
				;
			payload.flip();
			RowData rowData = new RowData(payload.array(), payload.position(),
					payload.limit());
			for (int p = rowData.getBufferStart(); p < rowData.getBufferEnd();) {
				rowData.prepareRow(p);
				scanCount++;
				if (VERBOSE) {
					System.out.println(String.format("%5d ", scanCount)
							+ rowData.toString(rowDefCache));
				}
				p = rowData.getRowEnd();
			}
		}
		if (VERBOSE) {
			System.out.println();
		}
		return scanCount;
	}

	int findIndexId(final RowDef groupRowDef, final RowDef userRowDef,
			final int fieldIndex) {
		final int findField = fieldIndex + userRowDef.getColumnOffset();
		for (final IndexDef indexDef : groupRowDef.getIndexDefs()) {
			if (indexDef.getFields().length == 1
					&& indexDef.getFields()[0] == findField) {
				return indexDef.getId();
			}
		}
		return -1;
	}

	public void testScanRows() throws Exception {
		populateTables();

		final RowDef rowDef = rowDefCache.getRowDef("_akiba_srt");
		final int fc = rowDef.getFieldCount();
		final RowData start = new RowData(new byte[256]);
		final RowData end = new RowData(new byte[256]);
		byte[] bitMap;

		{
			// Just the root table rows
			final RowDef userRowDef = rowDefCache.getRowDef("a");
			start.createRow(rowDef, new Object[fc]);
			end.createRow(rowDef, new Object[fc]);
			bitMap = bitsToRoot(userRowDef, rowDef);
			assertEquals(10, scanAllRows("all a", start, end, bitMap,
					0));
		}

		{
			final RowDef userRowDef = rowDefCache.getRowDef("aaaa");
			start.createRow(rowDef, new Object[fc]);
			end.createRow(rowDef, new Object[fc]);
			bitMap = bitsToRoot(userRowDef, rowDef);
			assertEquals(11110, scanAllRows("all aaaa", start, end, bitMap,
					findIndexId(rowDef, userRowDef, 1)));
		}

		{
			final RowDef userRowDef = rowDefCache.getRowDef("aaaa");
			int pkcol = columnOffset(userRowDef, rowDef);
			assertTrue(pkcol >= 0);
			pkcol += userRowDef.getPkFields()[0];
			Object[] startValue = new Object[fc];
			Object[] endValue = new Object[fc];
			startValue[pkcol] = 1;
			endValue[pkcol] = 2;
			start.createRow(rowDef, startValue);
			end.createRow(rowDef, endValue);
			bitMap = bitsToRoot(userRowDef, rowDef);
			assertEquals(5, scanAllRows("aaaa with aaaa.aaaa1 in [1,2]", start,
					end, bitMap, findIndexId(rowDef, userRowDef, 1)));
		}

		{
			final RowDef userRowDef = rowDefCache.getRowDef("aaaa");
			int pkcol = columnOffset(userRowDef, rowDef);
			assertTrue(pkcol >= 0);
			pkcol += userRowDef.getPkFields()[0];
			Object[] startValue = new Object[fc];
			Object[] endValue = new Object[fc];
			startValue[pkcol] = 100;
			endValue[pkcol] = 200;
			start.createRow(rowDef, startValue);
			end.createRow(rowDef, endValue);
			bitMap = bitsToRoot(userRowDef, rowDef);
			assertEquals(115, scanAllRows("aaaa with aaaa.aaaa1 in [100,200]",
					start, end, bitMap, findIndexId(rowDef, userRowDef, 1)));
		}

		{
			final RowDef userRowDef = rowDefCache.getRowDef("aaaa");
			final RowDef aaRowDef = rowDefCache.getRowDef("aa");
			int pkcol = columnOffset(aaRowDef, rowDef);
			assertTrue(pkcol >= 0);
			pkcol += aaRowDef.getPkFields()[0];
			Object[] startValue = new Object[fc];
			Object[] endValue = new Object[fc];
			startValue[pkcol] = 1;
			endValue[pkcol] = 5;
			start.createRow(rowDef, startValue);
			end.createRow(rowDef, endValue);
			bitMap = bitsToRoot(userRowDef, rowDef);
			assertEquals(556, scanAllRows("aaaa with aa.aa1 in [1,5]", start,
					end, bitMap, findIndexId(rowDef, aaRowDef, 1)));
		}

	}

	private int columnOffset(final RowDef userRowDef, final RowDef groupRowDef) {
		for (int i = groupRowDef.getUserTableRowDefs().length; --i >= 0;) {
			if (groupRowDef.getUserTableRowDefs()[i] == userRowDef) {
				return groupRowDef.getUserTableRowDefs()[i].getColumnOffset();
			}
		}
		return -1;
	}

	private byte[] bitsToRoot(final RowDef rowDef, final RowDef groupRowDef) {
		final byte[] bits = new byte[(groupRowDef.getFieldCount() + 7) / 8];
		for (RowDef rd = rowDef; rd != null;) {
			int level = -1;
			for (int i = 0; i < groupRowDef.getUserTableRowDefs().length; i++) {
				if (groupRowDef.getUserTableRowDefs()[i] == rd) {
					level = i;
					break;
				}
			}
			int column = groupRowDef.getUserTableRowDefs()[level]
					.getColumnOffset();
			bits[column / 8] |= 1 << (column % 8);
			if (rd.getParentRowDefId() == 0) {
				break;
			} else {
				rd = rowDefCache.getRowDef(rd.getParentRowDefId());
			}
		}
		return bits;
	}
}
