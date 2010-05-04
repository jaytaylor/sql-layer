package com.akiban.cserver.store;

import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_END_AT_EDGE;
import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_END_EXCLUSIVE;
import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_DESCENDING;
import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_PREFIX;
import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_SINGLE_ROW;
import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_START_AT_EDGE;
import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_START_EXCLUSIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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

public class ScanRowsTest implements CServerConstants {

	private final static File DATA_PATH = new File("/tmp/data");

	private final static String DDL_FILE_NAME = "src/test/resources/scan_rows_test.ddl";

	private final static boolean VERBOSE = false;

	private static PersistitStore store;

	private static RowDefCache rowDefCache;

	private static SortedMap<String, UserTable> tableMap = new TreeMap<String, UserTable>();

	private List<RowData> result = new ArrayList<RowData>();

	@BeforeClass
	public static void setUpSuite() throws Exception {

		rowDefCache = new RowDefCache();
		store = new PersistitStore(CServerConfig.unitTestConfig(), rowDefCache);
		CServerUtil.cleanUpDirectory(DATA_PATH);
		PersistitStore.setDataPath(DATA_PATH.getPath());
		final AkibaInformationSchema ais = new DDLSource()
				.buildAIS(DDL_FILE_NAME);
		rowDefCache.setAIS(ais);
		for (UserTable table : ais.getUserTables().values()) {
			tableMap.put(table.getName().getTableName(), table);
		}
		store.startUp();
		store.setOrdinals();
		new ScanRowsTest().populateTables();
	}

	@AfterClass
	public static void tearDownSuite() throws Exception {
		store.shutDown();
		store = null;
		rowDefCache = null;
		tableMap.clear();
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
				rowData.createRow(rowDef, new Object[] { (i / 10), i, 7, 8,
						i + "X" });
				assertEquals(OK, store.writeRow(rowData));
			}
		}
	}

	private int scanAllRows(final String test, final RowData start,
			final RowData end, final byte[] columnBitMap, final int indexId)
			throws Exception {
		return scanAllRows(test, start.getRowDefId(), 0, start, end,
				columnBitMap, indexId);
	}

	private int scanAllRows(final String test, final int rowDefId,
			final int scanFlags, final RowData start, final RowData end,
			final byte[] columnBitMap, final int indexId) throws Exception {
		int scanCount = 0;
		result.clear();
		final RowCollector rc = store.newRowCollector(rowDefId, indexId,
				scanFlags, start, end, columnBitMap);
		if (VERBOSE) {
			System.out.println("Test " + test);
		}
		while (rc.hasMore()) {
			final ByteBuffer payload = ByteBufferFactory.allocate(65536);
			while (rc.collectNextRow(payload))
				;
			payload.flip();
			for (int p = payload.position(); p < payload.limit();) {
				RowData rowData = new RowData(payload.array(), payload
						.position(), payload.limit());
				rowData.prepareRow(p);
				scanCount++;
				result.add(rowData);
				if (VERBOSE) {
					System.out.println(String.format("%5d ", scanCount)
							+ rowData.toString(rowDefCache));
				}
				p = rowData.getRowEnd();
			}
		}
		rc.close();
		if (VERBOSE) {
			System.out.println();
		}
		return scanCount;
	}

	int findIndexId(final RowDef groupRowDef, final RowDef userRowDef,
			final int fieldIndex) {
		final int findField = fieldIndex + userRowDef.getColumnOffset()
				- groupRowDef.getColumnOffset();
		for (final IndexDef indexDef : groupRowDef.getIndexDefs()) {
			if (indexDef.getFields().length == 1
					&& indexDef.getFields()[0] == findField) {
				return indexDef.getId();
			}
		}
		return -1;
	}

	int findIndexId(final RowDef rowDef, final String name) {
		for (final IndexDef indexDef : rowDef.getIndexDefs()) {
			if (indexDef.getName().equals(name)) {
				return indexDef.getId();
			}
		}
		return -1;
	}

	@Test
	public void testScanRows() throws Exception {
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
			assertEquals(10, scanAllRows("all a", start, end, bitMap, 0));
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
			int col = findFieldIndex(rowDef, "aa$aa1");
			int indexId = findIndexId(rowDef, rowDef.getTableName() + "$aa_PK");
			Object[] startValue = new Object[fc];
			Object[] endValue = new Object[fc];
			startValue[col] = 1;
			endValue[col] = 5;
			start.createRow(rowDef, startValue);
			end.createRow(rowDef, endValue);
			bitMap = bitsToRoot(userRowDef, rowDef);
			assertEquals(556, scanAllRows("aaaa with aa.aa1 in [1,5]", start,
					end, bitMap, indexId));
		}
	}

	@Test
	public void testScanFlags() throws Exception {
		final RowDef rowDef = rowDefCache.getRowDef("_akiba_srt");
		final int fc = rowDef.getFieldCount();
		final RowData start = new RowData(new byte[256]);
		final RowData end = new RowData(new byte[256]);
		byte[] bitMap;

		{
			final RowDef userRowDef = rowDefCache.getRowDef("a");
			bitMap = bitsToRoot(userRowDef, rowDef);
			assertEquals(10, scanAllRows("all a", userRowDef.getRowDefId(),
					SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE, null,
					null, bitMap, 0));
			assertEquals(10, scanAllRows("all a", userRowDef.getRowDefId(),
					SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE
							| SCAN_FLAGS_START_EXCLUSIVE
							| SCAN_FLAGS_END_EXCLUSIVE, null, null, bitMap, 0));
			assertEquals(1, scanAllRows("all a", userRowDef.getRowDefId(),
					SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE
							| SCAN_FLAGS_START_EXCLUSIVE
							| SCAN_FLAGS_END_EXCLUSIVE | SCAN_FLAGS_SINGLE_ROW,
					null, null, bitMap, 0));
			assertEquals(10, scanAllRows("all a", userRowDef.getRowDefId(),
					SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE
							| SCAN_FLAGS_START_EXCLUSIVE
							| SCAN_FLAGS_END_EXCLUSIVE | SCAN_FLAGS_DESCENDING,
					null, null, bitMap, 0));
		}

		{
			final RowDef userRowDef = rowDefCache.getRowDef("aaaa");
			bitMap = bitsToRoot(userRowDef, rowDef);
			assertEquals(11110, scanAllRows("all aaaa", rowDef.getRowDefId(),
					SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE, null,
					null, bitMap, findIndexId(rowDef, userRowDef, 1)));
			assertEquals(4, scanAllRows("all aaaa", rowDef.getRowDefId(),
					SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE
							| SCAN_FLAGS_SINGLE_ROW, null, null, bitMap,
					findIndexId(rowDef, userRowDef, 1)));
		}
		{
			final RowDef userRowDef = rowDefCache.getRowDef("aa");
			int col = findFieldIndex(rowDef, "aa$aa4");
			int indexId = findIndexId(rowDef, "aa$str");
			Object[] startValue = new Object[fc];
			Object[] endValue = new Object[fc];
			startValue[col] = "1";
			endValue[col] = "2";
			start.createRow(rowDef, startValue);
			end.createRow(rowDef, endValue);
			bitMap = bitsToRoot(userRowDef, rowDef);
			assertEquals(13, scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
					rowDef.getRowDefId(), 0, start, end, bitMap, indexId));
			assertEquals(13, scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
					rowDef.getRowDefId(), SCAN_FLAGS_START_EXCLUSIVE, start,
					end, bitMap, indexId));
			assertEquals(13, scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
					rowDef.getRowDefId(), SCAN_FLAGS_END_EXCLUSIVE, start, end,
					bitMap, indexId));
			assertEquals(2, scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
					rowDef.getRowDefId(), SCAN_FLAGS_END_EXCLUSIVE
							| SCAN_FLAGS_SINGLE_ROW, start, end, bitMap,
					indexId));
			assertEquals(2, scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
					rowDef.getRowDefId(), SCAN_FLAGS_SINGLE_ROW
							| SCAN_FLAGS_DESCENDING, start, end, bitMap,
					indexId));
			assertEquals(2, scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
					rowDef.getRowDefId(), SCAN_FLAGS_SINGLE_ROW
							| SCAN_FLAGS_DESCENDING | SCAN_FLAGS_PREFIX, start,
					end, bitMap, indexId));
			assertEquals(26, scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
					rowDef.getRowDefId(), SCAN_FLAGS_PREFIX, start, end,
					bitMap, indexId));
		}
	}

	@Test
	public void testCoveringIndex() throws Exception {
		final RowDef rowDef = rowDefCache.getRowDef("aaab");
		final int indexId = findIndexId(rowDef, "aaab3aaab1");
		final byte[] columnBitMap = new byte[1];
		columnBitMap[0] |= 1 << findFieldIndex(rowDef, "aaab1");
		columnBitMap[0] |= 1 << findFieldIndex(rowDef, "aaab3");
		final int count = scanAllRows("Covering index aaab3aaab1", rowDef
				.getRowDefId(), SCAN_FLAGS_START_AT_EDGE
				| SCAN_FLAGS_END_AT_EDGE, null, null, columnBitMap, indexId);
		assertTrue(count > 0);
	}

	private void assertOk(final int a, final int b) {
		System.out.println("AssertOk expected and got " + a + "," + b);
	}

	private int findFieldIndex(final RowDef rowDef, final String name) {
		for (int index = 0; index < rowDef.getFieldCount(); index++) {
			if (rowDef.getFieldDef(index).getName().equals(name)) {
				return index;
			}
		}
		return -1;
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

	private void scanLongTime() throws Exception {
		setUpSuite();
		populateTables();

		final RowData start = new RowData(new byte[256]);
		final RowData end = new RowData(new byte[256]);
		final RowDef rowDef = rowDefCache.getRowDef("aaaa");
		start.createRow(rowDef, new Object[] { null, 123 });
		end.createRow(rowDef, new Object[] { null, 132 });
		final int indexId = findIndexId(rowDef, rowDef, 1);
		byte[] bitMap = new byte[] { 0xF };
		System.out.println("Starting SELECT loop");
		long time = System.nanoTime();
		long iterations = 0;
		for (;;) {
			assertEquals(10, scanAllRows("one aaaa", start, end, bitMap,
					indexId));
			iterations++;
			if (iterations % 100 == 0) {
				long newtime = System.nanoTime();
				if (newtime - time > 5000000000L) {
					System.out.println(String.format("%10dns per iteration",
							(newtime - time) / iterations));
					iterations = 1;
					time = newtime;
				}
			}
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		for (final RowData rowData : result) {
			sb.append(rowData.toString(rowDefCache));
			sb.append(CServerUtil.NEW_LINE);
		}
		return sb.toString();
	}

	public static void main(final String[] args) throws Exception {
		final ScanRowsTest srt = new ScanRowsTest();
		srt.scanLongTime();
	}
}
