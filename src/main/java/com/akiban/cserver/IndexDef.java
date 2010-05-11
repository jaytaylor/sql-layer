package com.akiban.cserver;

import java.util.ArrayList;
import java.util.List;

/**
 * Defines an Index within the Chunk Server
 * 
 * 
 * @author peter
 * 
 */
public class IndexDef {

	private final String name;

	private final String treeName;

	private final int id;

	private final int[] fields;

	private final boolean primary;

	private final boolean unique;

	private final RowDef rowDef;

	private boolean hkeyEquivalent;

	private H2I[] indexKeyFields;

	private I2H[] hkeyFields;

	/**
	 * Structure that determines how a field in a table binds to a key segment
	 * of an index key. An H2I defines the field's position in an hkey and/or an
	 * h-row. For an index field that is not part of the row's primary key, the
	 * hKeyLoc property is -1.
	 */
	public static class H2I {

		int fieldIndex = -1;
		int hkeyLoc = -1;

		public String toString() {
			if (hkeyLoc == -1) {
				return "H2I<field=" + fieldIndex + ">";
			} else if (fieldIndex == -1) {
				return "H2I<hkeyLoc=" + hkeyLoc + ">";
			} else {
				return "H2I<field=" + fieldIndex + ",hkeyLoc=" + hkeyLoc + ">";
			}
		}

		public int getFieldIndex() {
			return fieldIndex;
		}

		public int getHkeyLoc() {
			return hkeyLoc;
		}

		public void setFieldIndex(int fieldIndex) {
			this.fieldIndex = fieldIndex;
		}

		public void setHkeyLoc(int hkeyLoc) {
			this.hkeyLoc = hkeyLoc;
		}
	}

	/**
	 * Structure that binds information about an index key segment to a h-row
	 * field. Instances are used when scanning rows by index. After an index key
	 * is selected, the I2H objects for that index are used in constructing the
	 * corresponding h-key to fetch the row.
	 * 
	 * As a special case, an instance of this class may represent a segment in
	 * the hkey where a table's ordinal id is written. (The ordinal is an
	 * identifier used to separate siblings in bushy trees.) This special case
	 * is recognized when fieldIndex and indexKeyLoc are both -1.
	 * 
	 */
	public static class I2H {

		final RowDef rowDef;
		int fieldIndex = -1;
		int indexKeyLoc = -1;

		private I2H(final RowDef rowDef) {
			this.rowDef = rowDef;
		}

		public String toString() {
			if (isOrdinalType()) {
				return "I2H<ordinal=" + rowDef.getOrdinal() + ">";
			} else if (indexKeyLoc == -1) {
				return "I2H<field=" + fieldIndex + ">";
			} else if (fieldIndex == -1) {
				return "I2H<indexLoc=" + indexKeyLoc + ">";
			} else {
				return "I2H<field=" + fieldIndex + ",indexLoc=" + indexKeyLoc
						+ ">";
			}
		}

		public int getFieldIndex() {
			return fieldIndex;
		}

		public int getIndexKeyLoc() {
			return indexKeyLoc;
		}

		public int getOrdinal() {
			return rowDef.getOrdinal();
		}

		public boolean isOrdinalType() {
			return fieldIndex == -1 && indexKeyLoc == -1;
		}

		public void setFieldIndex(int fieldIndex) {
			this.fieldIndex = fieldIndex;
		}

		public void setIndexKeyLoc(int indexKeyLoc) {
			this.indexKeyLoc = indexKeyLoc;
		}

	}

	public IndexDef(final String name, final RowDef rowDef,
			final String treeName, final int id, final int[] fields,
			final boolean primary, final boolean unique) {
		this.name = name;
		this.rowDef = rowDef;
		this.treeName = treeName;
		this.id = id;
		this.fields = fields;
		this.primary = primary;
		this.unique = unique;
	}

	public String getName() {
		return name;
	}

	public String getTreeName() {
		return treeName;
	}

	public int getId() {
		return id;
	}

	public int[] getFields() {
		return fields;
	}

	public boolean isPkIndex() {
		return primary;
	}

	/**
	 * True if this index represents fields matching the pkFields of the root
	 * table. If so, then there is no separately stored index tree.
	 * 
	 * @return
	 */
	public boolean isHKeyEquivalent() {
		return hkeyEquivalent;
	}

	public boolean isUnique() {
		return unique;
	}

	public RowDef getRowDef() {
		return rowDef;
	}

	public int getIndexKeySegmentCount() {
		return fields.length;
	}

	public H2I[] getIndexKeyFields() {
		return indexKeyFields;
	}

	public I2H[] getHkeyFields() {
		return hkeyFields;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder(name);
		sb.append("[");
		for (int i = 0; i < fields.length; i++) {
			sb.append(i == 0 ? "" : ",");
			sb.append(fields[i]);
		}
		sb.append("]->");
		sb.append(treeName);
		sb.append(":");
		sb.append(rowDef.getTableName());
		if (hkeyEquivalent) {
			sb.append("=hkey");
		}
		return sb.toString();
	}

	/**
	 * This complex method populates two arrays: hkeyFields and indexKeyFields.
	 * The elements of these arrays serve similar purposes: hkeyFields contains
	 * I2H elements which help the
	 * {@link com.akiban.cserver.store.PersistitStoreRowCollector} construct an
	 * h-key given an index key value; indexKeyFields contains H2I elements
	 * which help the PersistitStoreRowCollector construct an index key given
	 * HKey and RowData objects.
	 * 
	 * Once this method has finished, {@link #getHkeyFields()} returns an array
	 * containing one I2H object for each segment of the h-key, and
	 * {@link #getIndexKeyFields()} returns an array containing one H2I object
	 * for each segment of the index key.
	 * 
	 * This method also detects whether the index is "h-key equivalent", which
	 * means that instead of traversing this index, the RowCollector should
	 * instead traverse the h-keys themselves. The {@link #isHKeyEquivalent()}
	 * method returns this result.
	 * 
	 * This method must be called by the {@link com.akiban.cserver.RowDefCache}
	 * before the index can be used.
	 * 
	 * @param rowDefCache
	 * @param rowDef
	 * @param path
	 */
	void computeFieldAssociations(final RowDefCache rowDefCache,
			final RowDef rowDef, final List<RowDef> path) {

		//
		// Determine whether the fields in the index correspond exactly to the
		// fields in the h-table key for some table in the group. If so then
		// there is no need for a separate index. We mark it "hkeyEquivalent"
		// which prevents elements being field in an index tree and prevents
		// SELECT from selecting on this index. Note that SELECT can filter on
		// fields in the hkey, so there's no loss of filtering ability.
		//
		// TODO: Once we start implementing collation, character sets and other
		// elements that affect ordering, this test needs to verify that the
		// defined index matches the hkey's native ordering - otherwise this we
		// will need to store the index even though it contains the same fields
		// as the hkey.
		// 
		
		
		// TODO:
		// Detect that a non-PK field of a child row may be a PK field
		// of its parent.
		// 
		boolean matches = true;
		int at = 0;
		for (RowDef def : path) {
			if (at >= fields.length || !matches) {
				break;
			}
			final int[] pkFields = def.getPkFields();
			for (int j = 0; at < fields.length && j < pkFields.length; j++, at++) {
				int indexField = fields[at] + rowDef.getColumnOffset();
				int pkField = pkFields[j] + def.getColumnOffset();
				if (indexField != pkField) {
					matches = false;
					break;
				}
			}
		}

		if (at < fields.length) {
			matches = false;
		}

		if (matches) {
			hkeyEquivalent = true;
			// no need to do more - this IndexDef will not be used.
			return;
		}

		//
		// This index is not hkeyEquivalent; therefore set up the I2H and H2I
		// arrays for the RowCollector.
		//
		final List<I2H> i2hList = new ArrayList<I2H>();
		final List<H2I> h2iList = new ArrayList<H2I>();

		//
		// Start by adding an H2I for each field in the index.
		//

		for (int fieldIndex : fields) {
			final H2I h2i = new H2I();
			h2i.setFieldIndex(fieldIndex);
			h2iList.add(h2i);
		}

		//
		// Now set up the I2H list, and as a side-effect, add
		// needed information to the H2I objects.
		//
		for (final RowDef def : path) {
			//
			// This I2H is a placeholder to insert the RowDef's ordinal into
			// the h-key.
			//
			final I2H ordinalI2h = new I2H(def);
			i2hList.add(ordinalI2h);
			//
			// And then add an H2I for each pkField.
			//
			for (int i = 0; i < def.getPkFields().length; i++) {
				final int pkField = def.getPkFields()[i]
						+ (rowDef.isGroupTable() ? def.getColumnOffset() : 0);
				//
				// Search for an H2I already on the list that maps
				// to this field. This handles, for example, an pkField
				// that is also specified as a member of a secondary
				// index.
				//
				int indexLoc = -1;
				if (def == rowDef || rowDef.isGroupTable()) {
					for (int j = 0; j < h2iList.size(); j++) {
						final H2I h2i = h2iList.get(j);
						if (h2i.getFieldIndex() == pkField) {
							indexLoc = j;
							h2i.setHkeyLoc(i2hList.size());
							break;
						}
					}
				}
				//
				// If the H2I was not found above, then this pkField
				// needs to be added to the index key.
				//
				if (indexLoc == -1) {
					final H2I h2i = new H2I();
					h2i.setHkeyLoc(i2hList.size());
					if (def == rowDef) {
						h2i.setFieldIndex(pkField);
					}
					indexLoc = h2iList.size();
					h2iList.add(h2i);
				}
				//
				// Build the I2H that maps index field location to hkey
				//
				final I2H i2h = new I2H(null);

				if (def == rowDef) {
					i2h.setFieldIndex(pkField);
				}
				i2h.setIndexKeyLoc(indexLoc);
				i2hList.add(i2h);
			}
		}

		hkeyFields = i2hList.toArray(new I2H[i2hList.size()]);
		indexKeyFields = h2iList.toArray(new H2I[h2iList.size()]);
	}
}
