
package com.akiban.direct;

/**
 * A List expanded to accept selection, limit and sort capabilities.
 * 
 * @author peter
 *
 */
public interface DirectIterable<T> extends Iterable<T> {

    public DirectIterable<T> where(String predicate);

    public DirectIterable<T> where(String columnName, Object literal);

    public DirectIterable<T> sort(String sort);
    
    public DirectIterable<T> sort(String sort, String direction);
    
    public DirectIterable<T> limit(String limit);
    
    
}
