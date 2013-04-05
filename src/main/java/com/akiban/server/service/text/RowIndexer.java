/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.service.text;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.persistitadapter.PersistitHKey;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.util.PersistitKey;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.ShareHolder;
import com.persistit.Key;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;

/** Given <code>Row</code>s in hkey order, create <code>Document</code>s. */
public class RowIndexer implements Closeable
{
    private Map<RowType,Integer> ancestorRowTypes;
    private ShareHolder<Row>[] ancestors;
    private Set<RowType> descendantRowTypes;
    private Map<RowType,List<IndexedField>> fieldsByRowType;
    private IndexWriter writer;
    private Document currentDocument;
    private long documentCount;
    //private BytesRef keyBytes;
    private String keyEncodedString;
    private boolean updating;
    
    private static final Logger logger = LoggerFactory.getLogger(RowIndexer.class);

    public RowIndexer(FullTextIndexInfo index, IndexWriter writer, boolean updating) {
        UserTableRowType indexedRowType = index.getIndexedRowType();
        int depth = indexedRowType.userTable().getDepth();
        ancestorRowTypes = new HashMap<>(depth+1);
        ancestors = (ShareHolder<Row>[])new ShareHolder<?>[depth+1];
        fieldsByRowType = index.getFieldsByRowType();
        Set<RowType> rowTypes = index.getRowTypes();
        descendantRowTypes = new HashSet<>(rowTypes.size() - ancestorRowTypes.size());
        for (RowType rowType : rowTypes) {
            if ((rowType == indexedRowType) ||
                rowType.ancestorOf(indexedRowType)) {
                Integer ancestorDepth = ((UserTableRowType)rowType).userTable().getDepth();
                ancestorRowTypes.put(rowType, ancestorDepth);
                ancestors[ancestorDepth] = new ShareHolder<Row>();
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
            ancestors[ancestorDepth].hold(row);
            if (ancestorDepth == ancestors.length - 1) {
                addDocument();
                currentDocument = new Document();
                getKeyBytes(row);
                addFields(row, fieldsByRowType.get(rowType));
                for (int i = 0; i < ancestors.length - 1; i++) {
                    ShareHolder<Row> holder = ancestors[i];
                    if (holder != null) {
                        Row ancestor = holder.get();
                        if (ancestor != null) {
                            // We may have remembered an ancestor with no
                            // children and then this row is an orphan.
                            if (ancestor.ancestorOf(row)) {
                                addFields(ancestor, fieldsByRowType.get(ancestor.rowType()));
                            }
                            else {
                                holder.release();
                            }
                        }
                    }
                }
            }
        }
        else if (descendantRowTypes.contains(rowType)) {
            Row ancestor = ancestors[ancestors.length - 1].get();
            if ((ancestor != null) && ancestor.ancestorOf(row)) {
                addFields(row, fieldsByRowType.get(rowType));
            }
        }
    }
    
    public long indexRows(Cursor cursor) throws IOException {
        documentCount = 0;
        cursor.open();
        Row row;
        do {
            row = cursor.next();
            indexRow(row);
        } while (row != null);
        cursor.close();
        return documentCount;
    }

    protected void updateDocument(Cursor cursor, byte hkeyBytes[]) throws IOException
    {
        cursor.open();
        Row row = cursor.next();

        // Just the document correlating to this key
        // because 
        //      - If there exists one already, that will have been out-dated
        //      - If not, nothing happens.
        writer.deleteDocuments(new Term(IndexedField.KEY_FIELD, encodeBytes(hkeyBytes, 0, hkeyBytes.length)));

        if (row != null)
        {
            
            indexRow(null); // flush the last updated doc
            do
            {
                indexRow(row);
            }
            while ((row = cursor.next()) != null);
        }
        cursor.close();
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
        
        Field field = new StoredField(IndexedField.KEY_FIELD, keyEncodedString);
        currentDocument.add(field);
    }

    protected void addFields(Row row, List<IndexedField> fields) throws IOException {
        if (fields == null) return;
        for (IndexedField indexedField : fields) {
            PValueSource value = row.pvalue(indexedField.getPosition());
            Field field = indexedField.getField(value);
            currentDocument.add(field);
        }
    }

    static String encodeBytes(byte bytes[], int offset, int length)
    {
        // TODO: needs to be more efficient?
        String ret = Base64.encodeBase64String(Arrays.copyOfRange(bytes, offset, length));
        return ret;
    }
    
    static byte[] decodeString(String st)
    {
        return Base64.decodeBase64(st);
    }

    @Override
    public void close() {
        for (ShareHolder<Row> holder : ancestors) {
            if (holder != null) {
                holder.release();
            }
        }
    }

}
