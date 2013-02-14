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

package com.akiban.server.service.restdml;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.server.error.KeyColumnMismatchException;
import com.akiban.server.error.NoSuchColumnException;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PrimaryKeyParser {
    public static String STRING_ENCODING = "UTF8";

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
     */
    public static List<List<String>> parsePrimaryKeys(String pkList, Index primaryKey) {
        final int columnCount = primaryKey.getKeyColumns().size();
        try {
            List<List<String>> results = new ArrayList<>();
            String[] allPKs = pkList.split(";");
            for(String pk : allPKs) {
                String[] encodedColumns = pk.split(",");
                if(encodedColumns.length != columnCount) {
                    throw new KeyColumnMismatchException("Column count mismatch"); 
                }
                boolean colNameSpecified = false;
                String[] decodedColumns = new String[columnCount];
                for(int i = 0; i < columnCount; ++i) {
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
                    decodedColumns[pos] = URLDecoder.decode(value, STRING_ENCODING);
                }
                results.add(Arrays.asList(decodedColumns));
            }
            return results;
        } catch(UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static int positionInIndex(Index index, String columnName) {
        for(IndexColumn iCol : index.getKeyColumns()) {
            if(iCol.getColumn().getName().equals(columnName)) {
                return iCol.getPosition();
            }
        }
        throw new KeyColumnMismatchException ("Column `" + columnName + "` is not a primary key column");
    }
}
