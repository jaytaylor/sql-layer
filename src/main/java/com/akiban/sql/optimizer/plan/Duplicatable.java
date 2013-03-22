
package com.akiban.sql.optimizer.plan;

/** Somewhat like Cloneable, except that a deep copy is implied and it's possible
 * to request that the same object not be cloned twice in the new tree by keeping
 * it in a map.
 */
public interface Duplicatable
{
    public Duplicatable duplicate();
    public Duplicatable duplicate(DuplicateMap map);
}
