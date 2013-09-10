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

import com.persistit.Key;
import com.persistit.exception.ConversionException;

public class AkCollatorMySQL extends AkCollator {

    private static boolean TESTING = false;

    private final int[] weightTable = new int[256];
    private final int[] decodeTable = new int[256];

    private final boolean useKeyCoder;

    /**
     * Create an AkCollator which mimics single-byte case-insensitive MySQL
     * collations such as latin1_swedish_ci. This class derives the sort key for
     * collation by substituting weight values from the supplied table on a
     * per--character basis. The table supplied here is acquired from a
     * properties file:
     * 
     * src/main/resources/com/foundationdb/server/collation/collation_data_properties
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
     * @param collationId
     *            permanent (small) integer ID for collation scheme
     * @param table
     *            A table of byte values, formatted as a string consisting of
     *            space-delimited hex values.
     */
    AkCollatorMySQL(final String name, final String scheme, final int collationId, final String table, final boolean useKeyCoder) {
        super(name, scheme, collationId);
        constructWeightTable(table);
        this.useKeyCoder = useKeyCoder;
    }

    @Override
    public boolean isRecoverable() {
        return false;
    }

    @Override
    public void append(Key key, String value) {
        if (value == null) {
            key.append(null);
        } else if (useKeyCoder) {
            key.append(new CString(value, getCollationId()));
        } else {
            key.append(encodeSortKeyBytes(value));
        }
    }

    @Override
    public String decode(Key key)
    {
        if (TESTING) {
            String decoded;
            try {
                decoded = key.decode().toString();
            } catch (ConversionException ce) {
                decoded = key.decodeDisplayable(false);
            }
            return decoded;
        } else {
            throw new UnsupportedOperationException("Unable to decode a collator sort key");
        }
    }

    @Override
    public int compare(String source, String target) {
        int a = source.length();
        int b = target.length();
        for (int i = 0; i < a && i < b; i++) {
            int s = source.charAt(i);
            int t = target.charAt(i);
            assert s < 256 && t < 256;
            int d = weightTable[s] - weightTable[t];
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

    /**
     * Construct the sort key bytes for the given String value
     * 
     * @param value
     *            the String
     * @return sort key bytes
     */
    byte[] encodeSortKeyBytes(String value) {
        byte[] sortBytes = new byte[value.length()];
        for (int i = 0; i < value.length(); i++) {
            final int c = value.charAt(i);
            assert c < 256;
            int w = (byte) (weightTable[c] & 0xFF);
            assert w != 0;
            sortBytes[i] = (byte) w;
        }
        return sortBytes;
    }

    /**
     * Recover the approximate value. This method is used only in the
     * {@link CStringKeyCoder#renderKeySegment(Key, Object, Class, com.persistit.encoding.CoderContext)}
     * method. It supports the UI and makes approximate (case-incorrect)
     * decodings visible to within tools.
     */
    String decodeSortKeyBytes(byte[] bytes, int index, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = index; i < index + length; i++) {
            sb.append((char) (decodeTable[bytes[i] & 0xFF]));
        }
        return sb.toString();
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
            weightTable[i] = elements[i];
            if (decodeTable[elements[i]] == 0) {
                decodeTable[elements[i]] = i;
            }
        }
    }

    @Override
    public int hashCode(String value) {
        int result = 1;         // Compatible with Arrays.hashCode(byte[]).
        for (int i = 0; i < value.length(); i++) {
            final int c = value.charAt(i);
            assert c < 256;
            int w = (byte) (weightTable[c] & 0xFF);
            assert w != 0;
            result = result * 31 + (byte) w;
        }
        return result;
    }

    public static void useForTesting() {
        TESTING = true;
    }
}
