
package com.akiban.server.collation;

import com.akiban.server.service.tree.KeyCreator;
import com.persistit.Key;
import com.persistit.Persistit;

public class TestKeyCreator implements KeyCreator {

    public Key createKey() {
        return new Key((Persistit) null);
    }
}
