package com.akiba.cserver.store;

import java.io.File;

import junit.framework.TestCase;

import com.akiba.cserver.CServerConstants;
import com.akiba.cserver.FieldDef;
import com.akiba.cserver.FieldType;
import com.akiba.cserver.RowData;
import com.akiba.cserver.RowDef;
import com.akiba.cserver.RowDefCache;
import com.akiba.cserver.Util;
import com.persistit.Key;
import com.persistit.Persistit;

public class PersistitStoreWithAISTest extends TestCase implements CServerConstants {

	private final static File DATA_PATH = new File("/tmp/data");

	private PersistitStore store;

	private RowDefCache rowDefCache;

	@Override
	public void setUp() throws Exception {
		rowDefCache = new RowDefCache();
		store = new PersistitStore(rowDefCache);
		Util.cleanUpDirectory(DATA_PATH);
		PersistitStore.setDataPath(DATA_PATH.getPath());
		store.startUp();
		store.loadAIS(new File("src/test/resources/ais.sav"));
	}

	@Override
	public void tearDown() throws Exception {
		store.shutDown();
		store = null;
		rowDefCache = null;
	}


	public void testWriteCOIrows() throws Exception {
		final RowDef defC = rowDefCache.getRowDef(1);
		final RowDef defO = rowDefCache.getRowDef(2);
		final RowDef defI = rowDefCache.getRowDef(3);
		final RowData rowC = new RowData(new byte[64]);
		final RowData rowO = new RowData(new byte[64]);
		final RowData rowI = new RowData(new byte[64]);

		for (int c = 0; ++c <= 10;) {
			int cid = c;
			rowC.reset(0, 64);
			rowC.createRow(defC, new Object[] { cid, "Customer_" + cid });
			assertEquals(OK, store.writeRow(rowC));
			for (int o = 0; ++o <= 10;) {
				int oid = cid * 1000 + o;
				rowO.reset(0, 64);
				rowO.createRow(defO, new Object[] { oid, cid,
						12345});
				assertEquals(OK, store.writeRow(rowO));
				for (int i = 0; ++i <= 10;) {
					int iid = oid * 1000 + i;
					rowI.reset(0, 64);
					rowI.createRow(defI, new Object[] { oid, iid,
							123456, 654321});
					assertEquals(OK, store.writeRow(rowI));
				}
			}
		}

	}

}
