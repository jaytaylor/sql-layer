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
	
	public IndexDef(final String treeName, final int id, final int[] fields) {
		this.treeName = treeName;
		this.id = id;
		this.fields = fields;
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
}
