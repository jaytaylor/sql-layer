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

package com.akiban.server.service.text;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.ShareHolder;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/** Given <code>Row</code>s in hkey order, create <code>Document</code>s. */
public class RowIndexer implements Closeable
{
    private Map<RowType,Integer> ancestorRowTypes;
    private ShareHolder<Row>[] ancestors;
    private Set<RowType> descendantRowTypes;
    private Map<RowType,List<IndexedField>> fieldsByRowType;
    private IndexWriter writer;
    private Document currentDocument;
    
    private static final Logger logger = LoggerFactory.getLogger(RowIndexer.class);

    public RowIndexer(FullTextIndexAIS indexAIS, IndexWriter writer) {
        UserTableRowType indexedRowType = indexAIS.getIndexedRowType();
        int depth = indexedRowType.userTable().getDepth();
        ancestorRowTypes = new HashMap<>(depth+1);
        ancestors = (ShareHolder<Row>[])new ShareHolder<?>[depth+1];
        fieldsByRowType = indexAIS.getFieldsByRowType();
        Set<RowType> rowTypes = fieldsByRowType.keySet();
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
                assert false : "Not ancestor nor descendant " + rowType;
            }
        }
        this.writer = writer;
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
    
    public void indexRows(Cursor cursor) throws IOException {
        cursor.open();
        Row row;
        do {
            row = cursor.next();
            indexRow(row);
        } while (row != null);
        cursor.close();
    }

    protected void addDocument() throws IOException {
        if (currentDocument != null) {
            // TODO: To call updateDocument(), we need a Term for the
            // key, which won't work if there is more than one pkey
            // field. Maybe it should be a byte array of all of them
            // in Key format.
            writer.addDocument(currentDocument);
            logger.debug("Added {}", currentDocument);
            currentDocument = null;
        }
    }

    protected void addFields(Row row, List<IndexedField> fields) throws IOException {
        for (IndexedField indexedField : fields) {
            PValueSource value = row.pvalue(indexedField.getPosition());
            Field field = indexedField.getField(value);
            currentDocument.add(field);
        }
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
