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
package com.akiban.server.collation;

import com.akiban.server.types.ValueSource;
import com.ibm.icu.text.Collator;
import com.persistit.Key;

public class AkCollatorMySQL implements AkCollator {
    /*
     * TODO - reserve this in the Persistit Key class.
     */
    private final static byte TYPE_COLLATED_STRING = 127;

    private final String collatorName;

    private final String collatorScheme;

    private final int[] weight = new int[256];

    /**
     * Create an AkCollator which mimics single-byte case-insensitive MySQL
     * collations such as latin1_swedish_ci. This class derives the sort key for
     * collation by substituting weight values from the supplied table on a
     * per--character basis. The table supplied here is acquired from a
     * properties file:
     * 
     * src/main/resources/com/akiban/server/collation/collation_data_properties
     * 
     * See property keys such as "mysql_latin1_swedish_ci" for example.
     * 
     * This class is immutable and therefore does not supply or use a
     * thread-local Collator.
     * 
     * @param name
     *            Name, e.g., latin1_swedish_ci
     * @param scheme
     *            Scheme name, which for AkCollatorMySQL is used as a property
     *            name by which the supplied weight table is acquired.
     * @param table
     *            A table of byte values, formatted as a string consisting of
     *            space-delimited hex values.
     */
    AkCollatorMySQL(final String name, final String scheme, final String table) {
        this.collatorName = name;
        this.collatorScheme = scheme;
        constructWeightTable(table);
    }

    @Override
    public String getName() {
        return collatorName;
    }

    @Override
    public boolean isRecoverable() {
        return false;
    }

    @Override
    public void append(Key key, String value) {
        if (value == null) {
            key.append(null);
        } else {
            byte[] sortBytes = new byte[value.length() + 1];
            for (int i = 0; i < value.length(); i++) {
                final int c = value.charAt(i);
                assert c < 256;
                sortBytes[i] = (byte) (weight[c] & 0xFF);
            }
            byte[] keyBytes = key.getEncodedBytes();
            int size = key.getEncodedSize();
            if (size + sortBytes.length > key.getMaximumSize() + 1) {
                throw new IllegalArgumentException("Too long: " + size + sortBytes.length);
            }
            keyBytes[size] = TYPE_COLLATED_STRING;
            System.arraycopy(sortBytes, 0, keyBytes, size + 1, sortBytes.length);
            key.setEncodedSize(size + sortBytes.length + 1);
        }
    }

    @Override
    public String decode(Key key) {
        throw new UnsupportedOperationException("Unable to decode a collator sort key");
    }

    @Override
    public int compare(ValueSource value1, ValueSource value2) {
        return compare(value1.getString(), value2.getString());
    }

    @Override
    public int compare(String source, String target) {
        int a = source.length();
        int b = target.length();
        for (int i = 0; i < a && i < b; i++) {
            int s = source.charAt(i);
            int t = target.charAt(i);
            assert s < 256 && t < 256;
            int d = weight[s] - weight[t];
            if (d != 0) {
                return d;
            }
        }
        return a < b ? -1 : a > b ? 1 : 0;
    }

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public String toString() {
        return collatorName + "(" + collatorScheme + ")";
    }

    /**
     * Parse the table supplied a string value to populate the weight table. The
     * weight table provides a weight value for each of 256 characters. The
     * derived weight table may not contain zeroes because the weights are
     * directly substituted on a per-character basis to form sort keys.
     * 
     * @param table
     */
    private void constructWeightTable(final String table) {
        final int[] elements = new int[256];
        int count = 0;
        for (final String element : table.split(" ")) {
            if (element.length() == 2) {
                elements[count++] = Integer.parseInt(element, 16);
            }
        }
        assert count == 256;
        /*
         * Search for an unused weight value. There will always be at least one
         * in a case-insensitive table. We'll deal with non-CI tables when
         * necessary.
         */
        int hole = 0;
        for (; hole < 256; hole++) {
            boolean found = false;
            for (int index = 0; index < 256; index++) {
                if (elements[index] == hole) {
                    found = true;
                }
            }
            if (!found) {
                break;
            }
        }
        assert hole < 256;
        /*
         * Having found the first hole, increment all the weights below it. This
         * is how we ensure no zero-valued weights in the table.
         */
        for (int index = 0; index < 256; index++) {
            if (elements[index] < hole) {
                elements[index]++;
            }
        }
        for (int i = 0; i < 256; i++) {
            weight[i] = elements[i];
        }
    }
}
