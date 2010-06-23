/**
 * 
 */
package com.akiban.cserver.store;

/**
 * @author percent
 *
 */
public interface IVisitor {
    public void up();
    public void down();
    public boolean visit(Object node);
}
