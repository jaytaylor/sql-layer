package com.akiban.cserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.UserTable;

/**
 * Caches RowDef instances. In this incarnation, this class also constructs
 * RowDef objects from the AkibaInformationSchema. The translation is done in
 * the {@link #setAIS(AkibaInformationSchema)} method.
 * 
 * @author peter
 */
public class RowDefCache implements CServerConstants {

	private static final Log LOG = LogFactory.getLog(RowDefCache.class.getName());

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
			putRowDef(createUserTableRowDef(ais, table));
		}

		for (final GroupTable table : ais.getGroupTables().values()) {
			putRowDef(createGroupTableRowDef(ais, table));
		}
		
		if (LOG.isInfoEnabled()) {
			LOG.info(toString());
		}
	}
	
	private RowDef createUserTableRowDef(final AkibaInformationSchema ais, final UserTable table) {

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
			if (!index.getConstraint().equals("PRIMARY KEY")) {
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
		
		// root table
		UserTable root = table;
		while (root.getParentJoin() != null) {
			root = root.getParentJoin().getParent();
		}
		
		// group table name
		String groupTableName = null;
		for (final GroupTable groupTable : ais.getGroupTables().values()) {
			if (groupTable.getRoot().equals(root)) {
				groupTableName = groupTable.getName().getTableName();
			}
		}

		// Secondary indexes
		final List<IndexDef> indexDefList = new ArrayList<IndexDef>();
		for (final Index index : table.getIndexes()) {
			if (index.getConstraint().equals("PRIMARY KEY")) {
				continue;
			}
			final List<IndexColumn> indexColumns = index.getColumns();
			final List<Integer> indexColumnList = new ArrayList<Integer>(1);
			for (final IndexColumn indexColumn : indexColumns) {
				final int position = indexColumn.getPosition();
				indexColumnList.add(position);
			}

			int columnIndex = 0;
			int[] indexFields = new int[indexColumnList.size()];
			for (final Integer position : indexColumnList) {
				indexFields[columnIndex++] = position;
			}
			

			final String treeName = groupTableName + "$$" + index.getIndexId();
			final IndexDef indexDef = new IndexDef(treeName, index.getIndexId(), indexFields, index.isUnique());
			indexDefList.add(indexDef);
		}

		final RowDef rowDef = new RowDef(rowDefId, fieldDefs);
		rowDef.setTableName(table.getName().getTableName());
		rowDef.setTreeName(groupTableName);
		rowDef.setPkFields(pkFields);
		rowDef.setParentRowDefId(parentRowDefId);
		rowDef.setParentJoinFields(parentJoinFields);
		rowDef.setIndexDefs(indexDefList.toArray(new IndexDef[indexDefList.size()]));
		
		return rowDef;

	}
	
	private RowDef createGroupTableRowDef(final AkibaInformationSchema ais, final GroupTable table) {
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
				RowDef userRowDef = cacheMap.get(Integer.valueOf(userRowDefId));
				userRowDef.setGroupRowDefId(rowDefId);
			}
		}
		final int[] userRowDefIds = new int[userTableIndex];
		final int[] userRowColumnOffsets = new int[userTableIndex];
		
		System.arraycopy(tempRowDefIds, 0, userRowDefIds, 0, userTableIndex);
		System.arraycopy(tempRowColumnOffset, 0, userRowColumnOffsets, 0, userTableIndex);

		final RowDef rowDef = new RowDef(rowDefId, fieldDefs);
		rowDef.setTableName(table.getName().getTableName());
		rowDef.setTreeName( table.getName().getTableName());
		rowDef.setPkFields(new int[0]);
		rowDef.setUserRowDefIds(userRowDefIds);
		rowDef.setUserRowColumnOffsets(userRowColumnOffsets);
		
		return rowDef;
	}

	RowDef lookUpRowDef(final int rowDefId) {
		throw new UnsupportedOperationException("No RowDef for rowDefId="
				+ rowDefId);
	}

	/**
	 * Adds a RowDef preemptively to the cache. This is intended primarily to
	 * simplify unit tests.
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
		final StringBuilder sb = new StringBuilder("\n");
		for (Map.Entry<String, Integer> entry : nameMap.entrySet()) {
			final RowDef rowDef = cacheMap.get(entry.getValue());
			sb.append("   ");
			sb.append(rowDef);
			sb.append("\n");
		}
		return sb.toString();
	}
}
