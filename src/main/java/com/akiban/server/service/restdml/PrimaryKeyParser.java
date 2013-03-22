
package com.akiban.server.service.restdml;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.server.error.KeyColumnMismatchException;

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
