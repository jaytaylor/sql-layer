package com.akiba.cserver.store;

import java.io.File;

import junit.framework.TestCase;

import com.akiba.ais.ddl.DDLSource;
import com.akiba.ais.model.AkibaInformationSchema;
import com.akiba.cserver.CServerConstants;
import com.akiba.cserver.CServerUtil;
import com.akiba.cserver.RowData;
import com.akiba.cserver.RowDef;
import com.akiba.cserver.RowDefCache;

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

}
