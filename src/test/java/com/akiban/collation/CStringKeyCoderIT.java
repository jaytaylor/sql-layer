package com.akiban.collation;
import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import com.akiban.server.service.tree.TreeServiceImpl;
import com.akiban.server.test.it.ITBase;
import com.ibm.icu.text.Collator;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Persistit;

public class CStringKeyCoderIT  extends ITBase {

    final static String[] STRINGS = ("Now NOW now is IS Is the THE time for all good men to come to aid of"
            + " their party quick brown fox jumped over lazy red dog ").split("\\s");

    final static CStringKeyCoder KEY_CODER = new CStringKeyCoder();
    
    
    @Test
    public void testKeyCoder() throws Exception {
        final TreeServiceImpl treeService = (TreeServiceImpl) serviceManager().getTreeService();
        final Persistit persistit = treeService.getDb();
        persistit.getCoderManager().registerKeyCoder(CString.class, KEY_CODER);
        
        final Collator collator =  CollatorFactory.getCollator("en_US");

        final Key key = new Key(persistit);
        
        
        final SortedMap<KeyState, String> map = new TreeMap<KeyState, String>();
        for (final String s : STRINGS) {
            key.clear().append(1).append("abcde").append(new CString(s, collator)).append(42);
            final KeyState ks = new KeyState(key);
            String t = map.get(ks);
            if (t == null) {
                t = s;
            } else {
                t = t + ";" + s;
            }
            map.put(ks, t);
        }
        
        final CString decoded = new CString(collator);
        
        for (Map.Entry<KeyState, String> entry : map.entrySet()) {
            final KeyState ks = entry.getKey();
            ks.copyTo(key);
            System.out.printf("key=%20s value=%s\n", key, entry.getValue());
            key.indexTo(2);
            key.decode(decoded);
            final CString original = new CString(entry.getValue().split(";")[0], collator);
            assertEquals(0, original.compareTo(decoded));
        }
    }
}
