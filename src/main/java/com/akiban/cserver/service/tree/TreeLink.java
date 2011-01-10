package com.akiban.cserver.service.tree;

/**
 * Abstracts the information needed by TreeService to select an Exchange.
 * RowDef, IndexDef and other classes implement this interface.
 *  
 * @author peter
 *
 */
public interface TreeLink {
    /**
     * @return Schema name
     */
    String getSchemaName();
    /**
     * Tree name is the name of a Persistit B-Tree used to store a table,
     * index, or section of the schema.
     * 
     * @return tree name
     */
    String getTreeName();
    /**
     * Store an object (actually, a Persistit {@link com.persistit.Tree}
     * that TreeService uses to optimize access.  The type and identity of the
     * cached object is opaque to the implementing class.
     * 
     * @param object    The Object (actually a Tree) to save.
     */
    void setTreeCache(TreeCache cache);
    
    /**
     * Retrieve a cached Object.
     * 
     * @return the object
     */
    TreeCache getTreeCache();

}
