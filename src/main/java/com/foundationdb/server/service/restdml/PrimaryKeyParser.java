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

package com.foundationdb.server.service.restdml;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.server.error.KeyColumnMismatchException;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.util.Strings;
import com.foundationdb.util.WrappingByteSource;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PrimaryKeyParser {
    public static String STRING_ENCODING = "UTF8";
    public static String BINARY_SBCS_ENCODING = "LATIN1";

    /**
     * Parse a string containing a semi-colon separated list of PRIMARY KEYs.
     * <p>
     *     PRIMARY KEY: Comma separated list COLUMNs<br/>
     *     COLUMN: URL encoded field value OR URL encoded name=value pairs<br/>
     * </p>
     * <p>
     * For example,
     * <ul>
     *     <li>Single entity:</li>
     *     <ul>
     *         <li>/1</li>
     *          <li>/id=1</li>
     *     </ul>
     *     <li>Multiple entities:</li>
     *     <ul>
     *         <li>/1;2</li>
     *         <li>/id=1;id=2</li>
     *     </ul>
     *     <li>Single entity, multi-column PK:</li>
     *     <ul>
     *         <li>/1,1</li>
     *         <li>/oid=1,cid=1</li>
     *     </ul>
     *     <li>Multiple entities, multi-column PKs:</li>
     *     <ul>
     *         <li>/1,2;2,1</li>
     *         <li>/oid=1,cid=1;oid=2,cid=1</li>
     *     </ul>
     *  </ul>
     *  </p>
     *  <p>
     *  Binary columns can be URL-escaped or of the form <code>hex:digits</code> or
     *  <code>base64:encoded</code>.
     *  </p>
     */
    public static List<List<Object>> parsePrimaryKeys(String pkList, Index primaryKey) {
        final List<IndexColumn> keyColumns = primaryKey.getKeyColumns();
        final int columnCount = keyColumns.size();
        List<List<Object>> results = new ArrayList<>();
        String[] allPKs = pkList.split(";");
        for(String pk : allPKs) {
            String[] encodedColumns = pk.split(",");
            if(encodedColumns.length != columnCount) {
                throw new KeyColumnMismatchException("Column count mismatch"); 
            }
            boolean colNameSpecified = false;
            Object[] decodedColumns = new Object[columnCount];
            for(int i = 0; i < columnCount; ++i) {
                IndexColumn keyColumn = keyColumns.get(i);
                String[] pair = encodedColumns[i].split("=");
                final int pos;
                final String value;
                if(pair.length == 1) {
                    pos = i;
                    value = pair[0];
                    if(colNameSpecified) {
                        throw new KeyColumnMismatchException("Can not mix values with key/values");
                    }
                } else if(pair.length == 2) {
                    pos = positionInIndex(primaryKey, pair[0]);
                    value = pair[1];
                    if(i > 0 && !colNameSpecified) {
                        throw new KeyColumnMismatchException("Can not mix values with key/values");
                    }
                    colNameSpecified = true;
                } else {
                    throw new KeyColumnMismatchException ("Malformed column=value pair");
                }
                decodedColumns[pos] = decodeValue(keyColumn, value);
            }
            results.add(Arrays.asList(decodedColumns));
        }
        return results;
    }

    private static int positionInIndex(Index index, String columnName) {
        for(IndexColumn iCol : index.getKeyColumns()) {
            if(iCol.getColumn().getName().equals(columnName)) {
                return iCol.getPosition();
            }
        }
        throw new KeyColumnMismatchException ("Column `" + columnName + "` is not a primary key column");
    }

    private static Object decodeValue(IndexColumn keyColumn, String value) {
        try {
            if (keyColumn.getColumn().getType().typeClass() instanceof TBinary) {
                String[] pair = value.split(":");
                if (pair.length == 1) {
                    return new WrappingByteSource(URLDecoder.decode(value, BINARY_SBCS_ENCODING).getBytes(BINARY_SBCS_ENCODING));
                }
                else if (pair.length == 2) {
                    if ("hex".equals(pair[0])) {
                        return Strings.parseHexWithout0x(pair[1]);
                    }
                    else if ("base64".equals(pair[0])) {
                        return new WrappingByteSource(Strings.fromBase64(pair[1]));
                    }
                    else {
                        throw new KeyColumnMismatchException("Malformed encoding:encoded binary value"); 
                    }
                }
                else {
                    throw new KeyColumnMismatchException("Malformed encoding:encoded binary value"); 
                }
            }
            else {
                return URLDecoder.decode(value, STRING_ENCODING);
            }
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
