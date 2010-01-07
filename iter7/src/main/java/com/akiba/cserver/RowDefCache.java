package com.akiba.cserver;

import java.util.HashMap;
import java.util.Map;

/**
 * Caches RowDef instances.
 * 
 * @author peter
 */
public class RowDefCache {

	final Map<Integer, RowDef> cache = new HashMap<Integer, RowDef>();
	
	public synchronized RowDef getRowDef(final int rowDefId) {
		RowDef rowDef = cache.get(Integer.valueOf(rowDefId));
		if (rowDef == null) {
			rowDef = lookUpRowDef(rowDefId);
			cache.put(Integer.valueOf(rowDefId), rowDef);
		}
		return rowDef;
	}
	
	RowDef lookUpRowDef(final int rowDefId) {
		// TODO - supply an actual RowDef
		return new RowDef(rowDefId, new FieldDef[0]);
	}
}
