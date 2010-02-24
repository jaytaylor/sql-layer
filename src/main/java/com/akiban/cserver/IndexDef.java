package com.akiban.cserver;

/**
 * Defines an Index within the Chunk Server
 * 
 * @author peter
 *
 */
public class IndexDef {
	private final String treeName;
	private final int id;
	private final int[] fields;
	private final boolean unique;
	
	public IndexDef(final String treeName, final int id, final int[] fields, final boolean unique) {
		this.treeName = treeName;
		this.id = id;
		this.fields = fields;
		this.unique = unique;
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
	
	public boolean isUnique() {
		return unique;
	}
}
