
package com.akiban.direct;

/**
 * An extended RuntimeException needed tunnel SQLExceptions through the
 * Iterator interface.
 * 
 * @author peter
 *
 */
public class DirectException extends RuntimeException {

    private static final long serialVersionUID = 9217986178149864740L;

    public DirectException(final Exception e) {
        super(e);
    }

}
