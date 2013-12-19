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

package com.foundationdb.server.service.text;

import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.storeadapter.PersistitHKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.Strings;
import com.persistit.Key;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;

/** Given <code>Row</code>s in hkey order, create <code>Document</code>s. */
public class RowIndexer implements Closeable
{
    private Map<RowType,Integer> ancestorRowTypes;
    private Row[] ancestors;
    private Set<RowType> descendantRowTypes;
    private Map<RowType,List<IndexedField>> fieldsByRowType;
    private IndexWriter writer;
    private Document currentDocument;
    private long documentCount;
    private String keyEncodedString;
    private boolean updating;

    private static final Logger logger = LoggerFactory.getLogger(RowIndexer.class);

    public RowIndexer(FullTextIndexInfo index, IndexWriter writer, boolean updating) {
        TableRowType indexedRowType = index.getIndexedRowType();
        int depth = indexedRowType.table().getDepth();
        ancestorRowTypes = new HashMap<>(depth+1);
        ancestors = new Row[depth+1];
        fieldsByRowType = index.getFieldsByRowType();
        Set<RowType> rowTypes = index.getRowTypes();
        descendantRowTypes = new HashSet<>(rowTypes.size() - ancestorRowTypes.size());
        for (RowType rowType : rowTypes) {
            if ((rowType == indexedRowType) ||
                rowType.ancestorOf(indexedRowType)) {
                Integer ancestorDepth = rowType.table().getDepth();
                ancestorRowTypes.put(rowType, ancestorDepth);
            }
            else if (indexedRowType.ancestorOf(rowType)) {
                descendantRowTypes.add(rowType);
            }
            else {
                assert false : "Not ancestor or descendant " + rowType;
            }
        }
        this.writer = writer;
        this.updating = updating;
        currentDocument = null;
    }

    public void indexRow(Row row) throws IOException {
        if (row == null) {
            addDocument();
            return;
        }
        RowType rowType = row.rowType();
        Integer ancestorDepth = ancestorRowTypes.get(rowType);
        if (ancestorDepth != null) {
            ancestors[ancestorDepth] = row;
            if (ancestorDepth == ancestors.length - 1) {
                addDocument();
                currentDocument = new Document();
                getKeyBytes(row);
                addFields(row, fieldsByRowType.get(rowType));
                for (int i = 0; i < ancestors.length - 1; i++) {
                    Row ancestor = ancestors[i];
                    if (ancestor != null) {
                        // We may have remembered an ancestor with no
                        // children and then this row is an orphan.
                        if (ancestor.ancestorOf(row)) {
                            addFields(ancestor, fieldsByRowType.get(ancestor.rowType()));
                        }
                        else {
                            ancestors[i] = null;
                        }
                    }
                }
            }
        }
        else if (descendantRowTypes.contains(rowType)) {
            Row ancestor = ancestors[ancestors.length - 1];
            if ((ancestor != null) && ancestor.ancestorOf(row)) {
                addFields(row, fieldsByRowType.get(rowType));
            }
        }
    }
    
    public long indexRows(Cursor cursor) throws IOException {
        documentCount = 0;
        cursor.openTopLevel();
        Row row;
        do {
            row = cursor.next();
            indexRow(row);
        } while (row != null);
        cursor.closeTopLevel();
        return documentCount;
    }

    protected void updateDocument(Cursor cursor, byte hkeyBytes[]) throws IOException
    {
        if (indexRows(cursor) == 0)
        {
            String encoded = encodeBytes(hkeyBytes, 0, hkeyBytes.length);
            writer.deleteDocuments(new Term(IndexedField.KEY_FIELD, encoded));
            logger.debug("Deleted documents with encoded byptes: " + encoded);
        }
    }

    protected void addDocument() throws IOException {
        if (currentDocument != null) {
            if (updating) {
                
                writer.updateDocument(new Term(IndexedField.KEY_FIELD, keyEncodedString), 
                                      currentDocument);
                logger.debug("Updated {}", currentDocument);
            }
            else {
                writer.addDocument(currentDocument);
                logger.debug("Added {}", currentDocument);
            }
            documentCount++;
            currentDocument = null;
        }
    }

    protected void getKeyBytes(Row row) {
        Key key = ((PersistitHKey)row.hKey()).key();
        keyEncodedString = encodeBytes(key.getEncodedBytes(), 0, key.getEncodedSize());
        Field field = new StringField(IndexedField.KEY_FIELD, keyEncodedString, Store.YES);
        currentDocument.add(field);
    }

    protected void addFields(Row row, List<IndexedField> fields) throws IOException {
        if (fields == null) return;
        for (IndexedField indexedField : fields) {
            ValueSource value = row.value(indexedField.getPosition());
            Field field = indexedField.getField(value);
            currentDocument.add(field);
        }
    }

    static String encodeBytes(byte bytes[], int offset, int length)
    {
        // TODO: needs to be more efficient?
        return Strings.toBase64(bytes, offset, length);
    }
    
    static byte[] decodeString(String st)
    {
        return Strings.fromBase64(st);
    }

    @Override
    public void close() {
        Arrays.fill(ancestors, null);
    }

}
