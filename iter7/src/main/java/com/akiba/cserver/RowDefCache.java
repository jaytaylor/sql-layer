package com.akiba.cserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.akiba.ais.model.AkibaInformationSchema;
import com.akiba.ais.model.Column;
import com.akiba.ais.model.GroupTable;
import com.akiba.ais.model.Index;
import com.akiba.ais.model.IndexColumn;
import com.akiba.ais.model.Join;
import com.akiba.ais.model.JoinColumn;
import com.akiba.ais.model.UserTable;

/**
 * Caches RowDef instances. In this incarnation, this class also constructs
 * RowDef objects from the AkibaInformationSchema. The translation is done in
 * the {@link #setAIS(AkibaInformationSchema)} method.
 * 
 * @author peter
 */
public class RowDefCache implements CServerConstants {

	private final Map<Integer, RowDef> cacheMap = new HashMap<Integer, RowDef>();

	private final Map<String, Integer> nameMap = new TreeMap<String, Integer>();

	/**
	 * Look up and return a RowDef for a supplied rowDefId value.
	 * 
	 * @param rowDefId
	 * @return the corresponding RowDef
	 */
	public synchronized RowDef getRowDef(final int rowDefId) {
		RowDef rowDef = cacheMap.get(Integer.valueOf(rowDefId));
		if (rowDef == null) {
			rowDef = lookUpRowDef(rowDefId);
			cacheMap.put(Integer.valueOf(rowDefId), rowDef);
		}
		return rowDef;
	}

	public RowDef getRowDef(final String tableName) {
		final Integer key = nameMap.get(tableName);
		if (key == null) {
			return null;
		}
		return getRowDef(key.intValue());
	}

	/**
	 * Receive an instance of the AkibaInformationSchema, crack it and produce
	 * the RowDef instances it defines.
	 * 
	 * @param ais
	 */
	public synchronized void setAIS(final AkibaInformationSchema ais) {
		cacheMap.clear();
		nameMap.clear();
		for (final UserTable table : ais.getUserTables().values()) {

			// rowDefId
			final int rowDefId = table.getTableId();

			// FieldDef[]
			final FieldDef[] fieldDefs = new FieldDef[table.getColumns().size()];
			for (final Column column : table.getColumns()) {
				final String typeName = column.getType().name().toUpperCase();
				final FieldType type = FieldType.valueOf(typeName);
				final int fieldIndex = column.getPosition();
				if (type.isFixedWidth()) {
					fieldDefs[fieldIndex] = new FieldDef(column.getName(), type);
				} else {
					final Object typeParam = column.getTypeParameter1();
					fieldDefs[fieldIndex] = new FieldDef(column.getName(),
							type, ((Long) typeParam).intValue());
				}
			}

			// parentRowDef
			int parentRowDefId;
			int[] parentJoinFields;
			if (table.getParentJoin() != null) {
				final Join join = table.getParentJoin();
				final UserTable parentTable = join.getParent();
				parentRowDefId = parentTable.getTableId();
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
				parentRowDefId = 0;
				parentJoinFields = new int[0];
			}

			// pkFields
			int[] pkFields = null;
			for (final Index index : table.getIndexes()) {
				if (!index.isUnique()) {
					continue;
				}
				if (pkFields != null) {
					throw new IllegalStateException(
							"Can't handle two PK indexes on "
									+ table.getName().getTableName());
				}
				final List<IndexColumn> indexColumns = index.getColumns();
				final List<Integer> pkFieldList = new ArrayList<Integer>(1);
				for (final IndexColumn indexColumn : indexColumns) {
					final int position = indexColumn.getPosition();
					boolean isParentJoin = false;
					for (int i = 0; i < parentJoinFields.length; i++) {
						if (position == parentJoinFields[i]) {
							isParentJoin = true;
							break;
						}
					}
					if (!isParentJoin) {
						pkFieldList.add(position);
					}
				}

				int pkField = 0;
				pkFields = new int[pkFieldList.size()];
				for (final Integer position : pkFieldList) {
					pkFields[pkField++] = position;
				}
			}

			UserTable root = table;
			while (root.getParentJoin() != null) {
				root = root.getParentJoin().getParent();
			}
			String groupTableName = null;
			for (final GroupTable groupTable : ais.getGroupTables().values()) {
				if (groupTable.getRoot().equals(root)) {
					groupTableName = groupTable.getName().getTableName();
				}
			}

			final RowDef rowDef = RowDef.createRowDef(rowDefId, fieldDefs,
					table.getName().getTableName(), groupTableName, pkFields,
					parentRowDefId, parentJoinFields);
			putRowDef(rowDef);
		}

		for (final GroupTable table : ais.getGroupTables().values()) {
			// rowDefId
			final int rowDefId = table.getTableId();

			// FieldDef[]
			final FieldDef[] fieldDefs = new FieldDef[table.getColumns().size()];
			final Column[] columns = new Column[table.getColumns().size()];
			int[] tempRowDefIds = new int[columns.length];
			int[] tempRowColumnOffset = new int[columns.length];
			int userTableIndex = 0;
			int columnCount = 0;
			for (final Column column : table.getColumns()) {
				final int p = column.getPosition();
				if (columns[p] != null) {
					throw new IllegalStateException("Group table column "
							+ column.getName() + " has overlapping position "
							+ p);
				}
				columns[p] = column;
			}
			for (int position = 0; position < columns.length; position++) {
				final Column column = columns[position];
				final String typeName = column.getType().name().toUpperCase();
				final FieldType type = FieldType.valueOf(typeName);
				if (type.isFixedWidth()) {
					fieldDefs[position] = new FieldDef(column.getName(), type);
				} else {
					final Object typeParam = column.getTypeParameter1();
					fieldDefs[position] = new FieldDef(column.getName(), type,
							((Long) typeParam).intValue());
				}
				final Column userColumn = column.getUserColumn();
				if (userColumn.getPosition() == 0) {
					int userRowDefId = userColumn.getTable().getTableId();
					tempRowDefIds[userTableIndex] = userRowDefId;
					tempRowColumnOffset[userTableIndex] = columnCount;
					userTableIndex++;
					columnCount += userColumn.getTable().getColumns().size();
				}
			}
			final int[] userRowDefIds = new int[userTableIndex];
			final int[] userRowColumnOffsets = new int[userTableIndex];
			
			System.arraycopy(tempRowDefIds, 0, userRowDefIds, 0, userTableIndex);
			System.arraycopy(tempRowColumnOffset, 0, userRowColumnOffsets, 0, userTableIndex);

			final RowDef rowDef = RowDef.createRowDef(rowDefId, fieldDefs,
					table.getName().getTableName(), table.getName()
							.getTableName(), new int[0]);
			rowDef.setUserRowDefIds(userRowDefIds);
			rowDef.setUserRowColumnOffsets(userRowColumnOffsets);
			putRowDef(rowDef);

		}
	}

	RowDef lookUpRowDef(final int rowDefId) {
		throw new UnsupportedOperationException("No RowDef for rowDefId="
				+ rowDefId);
	}

	/**
	 * Adds a RowDef preemptively to the cache. This is intended primarily to
	 * simply unit tests.
	 * 
	 * @param rowDef
	 */
	public synchronized void putRowDef(final RowDef rowDef) {
		Integer key = Integer.valueOf(rowDef.getRowDefId());
		if (cacheMap.containsKey(key)
				|| nameMap.containsKey(rowDef.getTableName())) {
			throw new IllegalStateException("RowDef " + rowDef
					+ " already exists");
		}
		cacheMap.put(key, rowDef);
		nameMap.put(rowDef.getTableName(), key);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Integer> entry : nameMap.entrySet()) {
			final RowDef rowDef = cacheMap.get(entry.getValue());
			sb.append(rowDef);
			sb.append("\n");
		}
		return sb.toString();
	}
}
