package com.akiba.cserver.store;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.TestCase;

import com.akiba.ais.ddl.DDLSource;
import com.akiba.ais.model.AkibaInformationSchema;
import com.akiba.ais.model.UserTable;
import com.akiba.cserver.CServerConstants;
import com.akiba.cserver.CServerUtil;
import com.akiba.cserver.RowData;
import com.akiba.cserver.RowDef;
import com.akiba.cserver.RowDefCache;
import com.akiba.util.ByteBufferFactory;
import com.persistit.Volume;

public class ScanRowsTest extends TestCase implements CServerConstants {

	private final static File DATA_PATH = new File("/tmp/data");

	private final static String DDL_FILE_NAME = "src/test/resources/scan_rows_test.ddl";

	private final static int[] TENS = new int[] { 1, 10, 100, 1000, 10000,
			100000, 1000000, 10000000 };

	private PersistitStore store;

	private RowDefCache rowDefCache;

	private SortedMap<String, UserTable> tableMap = new TreeMap<String, UserTable>();

	@Override
	public void setUp() throws Exception {
		rowDefCache = new RowDefCache();
		store = new PersistitStore(rowDefCache);
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
		for (String name : tableMap.keySet()) {
			final RowDef rowDef = rowDefCache.getRowDef(name);
			final int level = name.length();
			int k = (int) Math.pow(10, level);
			for (int i = 0; i < k; i++) {
				rowData.createRow(rowDef, new Object[] {
						(i / 10) * TENS[level - 1], i * TENS[level], 7, 8 });
				assertEquals(OK, store.writeRow(rowData));
			}
		}
	}

	private int scanAllRows(final RowData start, final RowData end,
			final byte[] columnBitMap) throws Exception {
		int scanCount = 0;
		final RowCollector rc = store.newRowCollector(1111, start, end,
				columnBitMap);
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
		return scanCount;
	}

	public void testScanRows() throws Exception {
		populateTables();

		final RowDef rowDef = rowDefCache.getRowDef("_akiba_srt");
		final RowData start = new RowData(new byte[256]);
		final RowData end = new RowData(new byte[256]);
		byte[] bitMap;

		{
			// Just the root table rows
			final RowDef userRowDef = rowDefCache.getRowDef("a");
			start.createRow(rowDef, new Object[] {});
			end.createRow(rowDef, new Object[] {});
			bitMap = bitsToRoot(userRowDef, rowDef);
			assertEquals(10, scanAllRows(start, end, bitMap));
		}

		{
			final RowDef userRowDef = rowDefCache.getRowDef("aaaa");
			start.createRow(rowDef, new Object[] {});
			end.createRow(rowDef, new Object[] {});
			bitMap = bitsToRoot(userRowDef, rowDef);
			assertEquals(11110, scanAllRows(start, end, bitMap));
		}
	}

	private byte[] bitsToRoot(final RowDef rowDef, final RowDef groupRowDef) {
		final byte[] bits = new byte[(groupRowDef.getFieldCount() + 7) / 8];
		for (RowDef rd = rowDef; rd != null;) {
			int level = -1;
			for (int i = 0; i < groupRowDef.getUserRowDefIds().length; i++) {
				if (groupRowDef.getUserRowDefIds()[i] == rd.getRowDefId()) {
					level = i;
					break;
				}
			}
			int column = groupRowDef.getUserRowColumnOffsets()[level];
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
