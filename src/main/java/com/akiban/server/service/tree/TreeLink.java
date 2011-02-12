/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.service.tree;

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
