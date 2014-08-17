/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.collation;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.junit.Ignore;
import org.junit.Test;

import com.foundationdb.server.test.it.ITBase;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Persistit;

public class AkCollatorIcuIT extends ITBase {

    /**
     * Sequences generated from MySQL on InnoDB engine using a script like this:
     * 
     * <code><pre>
     *    drop table if exists tc_innodb_latin1_swedish_ci;
     *    
     *    create table tc_innodb_latin1_swedish_ci(a int primary key, b varchar(200) 
     *         character set latin1 collate latin1_swedish_ci) engine innodb;
     *         
     *    create index c on tc_innodb_latin1_swedish_ci (b);
     *    
     *    insert into tc_innodb_latin1_swedish_ci values 
     *    (0, X'00'),
     *    (1, X'01'),
     *    ...
     *    (254, X'FE'),
     *    (255, X'FF');
     * 
     *    select a from tc_innodb_latin1_swedish_ci order by b into outfile 
     *       '/tmp/collation/r_tc_innodb_latin1_swedish_ci.innodb';
     * </pre></code>
     */
    private final static Integer[] LATIN1_GENERAL_CI_SEQ = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
            17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43,
            44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 97, 192, 224, 193,
            225, 194, 226, 195, 227, 196, 228, 197, 229, 198, 230, 66, 98, 67, 99, 199, 231, 68, 100, 208, 240, 69,
            101, 200, 232, 201, 233, 202, 234, 203, 235, 70, 102, 71, 103, 72, 104, 73, 105, 204, 236, 205, 237, 206,
            238, 207, 239, 74, 106, 75, 107, 76, 108, 77, 109, 78, 110, 209, 241, 79, 111, 210, 242, 211, 243, 212,
            244, 213, 245, 214, 246, 216, 248, 80, 112, 81, 113, 82, 114, 83, 115, 223, 84, 116, 85, 117, 217, 249,
            218, 250, 219, 251, 220, 252, 86, 118, 87, 119, 88, 120, 89, 121, 221, 253, 255, 90, 122, 222, 254, 91, 92,
            93, 94, 95, 96, 123, 124, 125, 126, 215, 247, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138,
            139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
            160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180,
            181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, };

    private final static Integer[] LATIN1_SWEDISH_CI_SEQ = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
            17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43,
            44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 97, 192, 193, 194,
            195, 224, 225, 226, 227, 66, 98, 67, 99, 199, 231, 68, 100, 208, 240, 69, 101, 200, 201, 202, 203, 232,
            233, 234, 235, 70, 102, 71, 103, 72, 104, 73, 105, 204, 205, 206, 207, 236, 237, 238, 239, 74, 106, 75,
            107, 76, 108, 77, 109, 78, 110, 209, 241, 79, 111, 210, 211, 212, 213, 242, 243, 244, 245, 80, 112, 81,
            113, 82, 114, 83, 115, 84, 116, 85, 117, 217, 218, 219, 249, 250, 251, 86, 118, 87, 119, 88, 120, 89, 121,
            220, 221, 252, 253, 90, 122, 91, 197, 229, 92, 196, 198, 228, 230, 93, 214, 246, 94, 95, 96, 123, 124, 125,
            126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146,
            147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167,
            168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188,
            189, 190, 191, 215, 216, 248, 222, 254, 223, 247, 255, };

    @Test
    public void keyComparisons() throws Exception {
        final AkCollator collator = AkCollatorFactory.getAkCollator("latin1_swedish_ci");
        assertEquals("Should be case-insensitive", 0, collator.compare("abc", "ABC"));
        assertTrue("Should collate after", collator.compare("BCD", "abc") > 0);
        assertTrue("Should collate before", collator.compare("abc", "BCD") < 0);
        assertTrue("Should collate after", collator.compare("BCD", "bc") > 0);
        assertTrue("Should collate before", collator.compare("ABCD", "abcde") < 0);
    }

    @Test
    public void keyEncoding() throws Exception {
        final AkCollator collator = AkCollatorFactory.getAkCollator("latin1_swedish_ci");
        final Key key1 = store().createKey();
        final Key key2 = store().createKey();
        collator.append(key1, "abc");
        collator.append(key2, "abc");
        assertTrue("Keys should compare equal", key1.compareTo(key2) == 0);
        collator.append(key2.clear(), "BCD");
        assertTrue("First key should be less", key1.compareTo(key2) < 0);
        collator.append(key1.clear(), "def");
        assertTrue("First key should be less", key1.compareTo(key2) > 0);
        collator.append(key2.clear(), "defg");
        assertTrue("First key should be less", key1.compareTo(key2) < 0);
        collator.append(key2.clear(), "de");
        assertTrue("First key should be less", key1.compareTo(key2) > 0);
    }

    /*@Ignore("Requires CString registered to Key")
    @Test
    public void keyDecoding() throws Exception {
        final AkCollator collator = AkCollatorFactory.getAkCollator("latin1_swedish_ci");
        final Key key = store().createKey();
        key.append(new CString("aBcDe123!@#$%^&*(()", collator.getCollationId()));
        assertEquals("Incorrect display form", "{(com.foundationdb.server.collation.CString)ABCDE123!@#$%^&*(()}", key.toString());
    }*/

    @Test
    public void keyEncodingMatchesMySQL() throws Exception {
        verifySequence("latin1_swedish_ci", LATIN1_SWEDISH_CI_SEQ);
        verifySequence("latin1_general_ci", LATIN1_GENERAL_CI_SEQ);
    }

    private void verifySequence(final String collation, final Integer[] expected) {
        final AkCollator collator = AkCollatorFactory.getAkCollator(collation);
        final Key key = store().createKey();
        final Map<KeyState, Integer> map = new TreeMap<>();
        for (int i = 0; i < 256; i++) {
            final String s = new String(new char[] { (char) i });
            collator.append(key.clear(), s);
            key.append(i);
            map.put(new KeyState(key), i);
        }
        assertArrayEquals("Key sequence should match output generated by InnoDB", expected, map.values().toArray(
                new Integer[256]));
    }

    @Test
    public void randomStrings() throws Exception {
        testRandomStringSequence("latin1_swedish_ci", 10000, LATIN1_SWEDISH_CI_SEQ);
        testRandomStringSequence("latin1_general_ci", 10000, LATIN1_GENERAL_CI_SEQ);
    }

    private void testRandomStringSequence(final String collation, final int count, final Integer[] table)
            throws Exception {
        final AkCollator collator = AkCollatorFactory.getAkCollator(collation);
        final Map<String, Integer> map1 = new TreeMap<>(new Comparator<String>() {
            @Override
            public int compare(String source, String target) {
                return collator.compare(source, target);
            }
        });
        final Map<KeyState, Integer> map2 = new TreeMap<>();
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        final Key key = store().createKey();
        for (int i = 0; i < count; i++) {
            sb.setLength(0);
            key.clear();
            for (int j = 0; j <= random.nextInt(5); j++) {
                sb.append((char) random.nextInt(256));
            }
            // tie breaker for consistency
            sb.append(String.format("_%08d", i));
            String s = sb.toString();
            map1.put(s, i);
            collator.append(key, s);
            key.append(i);
            map2.put(new KeyState(key), i);
        }
        Integer[] a1 = map1.values().toArray(new Integer[map1.size()]);
        Integer[] a2 = map1.values().toArray(new Integer[map2.size()]);
        assertArrayEquals("Comparators should match", a1, a2);
    }

}
