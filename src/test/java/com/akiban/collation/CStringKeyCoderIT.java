/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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

public class CStringKeyCoderIT extends ITBase {

    final static String[] STRINGS = ("Now NOW now is IS Is the THE time for all good men to come to aid of"
            + " their party quick brown fox jumped over lazy red dog ").split("\\s");

    final static CStringKeyCoder KEY_CODER = new CStringKeyCoder();

    @Test
    public void testKeyCoder() throws Exception {
        final TreeServiceImpl treeService = (TreeServiceImpl) serviceManager().getTreeService();
        final Persistit persistit = treeService.getDb();
        persistit.getCoderManager().registerKeyCoder(CString.class, KEY_CODER);

        final Collator collator = CollatorFactory.getCollator("en_US");

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

    final static int ITERATIONS = 20000000;

    @Test
    public void bench() throws Exception {
        final TreeServiceImpl treeService = (TreeServiceImpl) serviceManager().getTreeService();
        final Persistit persistit = treeService.getDb();
        persistit.getCoderManager().registerKeyCoder(CString.class, KEY_CODER);

        final Collator collator = CollatorFactory.getCollator("en_US");

        final Key key = new Key(persistit);
        final CString decoded = new CString(collator);
        final StringBuilder sb = new StringBuilder();

        long time1 = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            String s = STRINGS[i % STRINGS.length];
            CString encoded = new CString(s, collator);
            key.clear().append(1).append("abcde").append(encoded).append(42);
            key.indexTo(2);
            key.decode(decoded);
        }
        time1 = System.nanoTime() - time1;

        long time2 = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            String s = STRINGS[i % STRINGS.length];
            key.clear().append(1).append("abcde").append(s).append(42);
            key.indexTo(2);
            key.decode(sb);
        }
        time2 = System.nanoTime() - time2;

        System.out.printf("per iteration=%,dns with Collator, %dns without Collator\n", time1 / ITERATIONS, time2
                / ITERATIONS);
    }
}
