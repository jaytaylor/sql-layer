package com.akiba.cserver;

import java.util.HashMap;
import java.util.Map;

import com.akiba.ais.model.AkibaInformationSchema;
import com.akiba.ais.model.Column;
import com.akiba.ais.model.Join;
import com.akiba.ais.model.JoinColumn;
import com.akiba.ais.model.UserTable;

/**
 * Caches RowDef instances.
 * 
 * @author peter
 */
public class RowDefCache {

	private final Map<Integer, RowDef> cache = new HashMap<Integer, RowDef>();

	private AkibaInformationSchema ais;

	public synchronized RowDef getRowDef(final int rowDefId) {
		RowDef rowDef = cache.get(Integer.valueOf(rowDefId));
		if (rowDef == null) {
			rowDef = lookUpRowDef(rowDefId);
			cache.put(Integer.valueOf(rowDefId), rowDef);
		}
		return rowDef;
	}

	public void setAIS(final AkibaInformationSchema ais) {
		this.ais = ais;
		for (final UserTable table : ais.getUserTables().values()) {

			// rowDefId
			final int rowDefId = table.getTableId();

			// FieldDef[]
			final FieldDef[] fieldDefs = new FieldDef[table.getColumns().size()];
			int fieldIndex = 0;
			for (final Column column : table.getColumns()) {
				final String typeName = column.getTypeName().toUpperCase();
				final Object typeParam = column.getTypeParameter1();
				final FieldType type = FieldType.valueOf(typeName);
				fieldDefs[fieldIndex] = type.isFixedWidth() ? new FieldDef(type)
						: new FieldDef(type, ((Long) typeParam).intValue());
				fieldIndex++;
			}
			
			// pkFields -- TODO  NEED HELP
			int[] pkfields = new int[]{0};

			// parentRowDef
			int parentRowDef;
			int[] parentJoinFields;
			if (table.getParentJoin() != null) {
				final Join join = table.getParentJoin();
				final UserTable parentTable = join.getParent();
				parentRowDef = parentTable.getTableId();
				//
				// parentJoinFields - TODO - not sure this is right.
				//
				parentJoinFields = new int[join.getJoinColumns().size()];
				for (int index = 0; index < join.getJoinColumns().size(); index++) {
					final JoinColumn joinColumn = join.getJoinColumns().get(
							index);
					parentJoinFields[index] = joinColumn.getChild()
							.getPosition();
				}
			} else {
				parentRowDef = 0;
				parentJoinFields = new int[0];
			}
			
			
			final RowDef rowDef = RowDef.createRowDef(rowDefId, fieldDefs,
					table.getTableName(), pkfields, parentRowDef,
					parentJoinFields);
			putRowDef(rowDef);
		}
	}

	RowDef lookUpRowDef(final int rowDefId) {
		return new RowDef(rowDefId, new FieldDef[0]);
	}

	/**
	 * Adds a RowDef preemptively to the cache. This is intended primarily to
	 * simply unit tests.
	 * 
	 * @param rowDef
	 */
	public synchronized void putRowDef(final RowDef rowDef) {
		cache.put(Integer.valueOf(rowDef.getRowDefId()), rowDef);
	}
}
