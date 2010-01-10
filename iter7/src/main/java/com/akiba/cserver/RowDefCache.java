package com.akiba.cserver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.akiba.ais.model.AkibaInformationSchema;
import com.akiba.ais.model.Column;
import com.akiba.ais.model.Index;
import com.akiba.ais.model.IndexColumn;
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
			for (final Column column : table.getColumns()) {
				final String typeName = column.getTypeName().toUpperCase();
				final Object typeParam = column.getTypeParameter1();
				final FieldType type = FieldType.valueOf(typeName);
				final int fieldIndex = column.getPosition();
				fieldDefs[fieldIndex] = type.isFixedWidth() ? new FieldDef(type)
						: new FieldDef(type, ((Long) typeParam).intValue());
			}
			
			// pkFields
			int[] pkFields = null;
			for (final Index index : table.getIndexes()) {
				if (!index.isUnique()) {
					continue;
				}
				if (pkFields != null) {
					throw new IllegalStateException("Can't handle two PK indexes on " + table.getTableName());
				}
				final List<IndexColumn> indexColumns = index.getColumns();
				pkFields = new int[indexColumns.size()];
				int pkField = 0;
				for (final IndexColumn indexColumn : indexColumns) {
					pkFields[pkField++] = indexColumn.getPosition();
				}
			}

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
					table.getTableName(), pkFields, parentRowDef,
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
