
package com.akiban.server.service.tree;

import com.persistit.Key;

/**
 * Create new, empty Persistit Key instances correctly populated with for
 * decoding.
 * 
 * @author peter
 *
 */
public interface KeyCreator {

    Key createKey();
    
}
