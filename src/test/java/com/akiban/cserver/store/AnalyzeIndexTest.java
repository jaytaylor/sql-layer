package com.akiban.cserver.store;

import org.junit.Test;

import com.akiban.cserver.IndexDef;
import com.akiban.cserver.RowDef;

public class AnalyzeIndexTest extends AbstractScanBase {

	@Test
	public void testAnalyzeAllIndexes() throws Exception {
		for (final RowDef rowDef : rowDefCache.getRowDefs()) {
			for (final IndexDef indexDef : rowDef.getIndexDefs()) {
				store.getIndexManager().analyzeIndex(indexDef, 10);
			}
		}
	}
}
