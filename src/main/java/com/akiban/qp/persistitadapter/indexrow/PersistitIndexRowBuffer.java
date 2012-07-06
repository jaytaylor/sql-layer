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

package com.akiban.qp.persistitadapter.indexrow;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.IndexToHKey;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.row.IndexRow;
import com.akiban.qp.util.PersistitKey;
import com.akiban.server.PersistitKeyPValueSource;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.store.PersistitKeyAppender;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;

import static java.lang.Math.min;

public class PersistitIndexRowBuffer extends IndexRow
{
    // BoundExpressions interface

    public final int compareTo(BoundExpressions row, int leftStartIndex, int rightStartIndex, int fieldCount)
    {
        PersistitIndexRowBuffer that = (PersistitIndexRowBuffer) row;
        Key thisKey = this.keyAppender.key();
        Key thatKey = that.keyAppender.key();
        int thisPosition = thisKey.indexTo(leftStartIndex).getIndex();
        int thatPosition = thatKey.indexTo(rightStartIndex).getIndex();
        int thisEnd = thisKey.indexTo(leftStartIndex + fieldCount).getIndex();
        int thatEnd = thatKey.indexTo(rightStartIndex + fieldCount).getIndex();
        byte[] thisBytes = thisKey.getEncodedBytes();
        byte[] thatBytes = thatKey.getEncodedBytes();
        int thisByteCount = thisEnd - thisPosition;
        int thatByteCount = thatEnd - thatPosition;
        int thisStop = thisPosition + min(thisByteCount, thatByteCount);
        int c = 0;
        int eqSegments = 0;
        while (thisPosition < thisStop) {
            byte thisByte = thisBytes[thisPosition++];
            byte thatByte = thatBytes[thatPosition++];
            c = thisByte - thatByte;
            if (c != 0) {
                break;
            } else if (thisByte == 0) {
                // thisByte = thatByte = 0
                eqSegments++;
            }
        }
        // If c == 0 then thisPosition == thisStop and the two subarrays must match.
        if (c < 0) {
            c = -(eqSegments + 1);
        } else if (c > 0) {
            c = eqSegments + 1;
        }
        return c;
    }

    // IndexRow interface

    public void append(FieldDef fieldDef, RowData rowData)
    {
        keyAppender.append(fieldDef, rowData);
    }

    public void append(Column column, ValueSource source)
    {
        keyAppender.append(source, column);
    }

    public void appendFieldFromKey(Key fromKey, int depth)
    {
        keyAppender.appendFieldFromKey(fromKey, depth);
    }

    public boolean keyEmpty()
    {
        return keyAppender.key().getEncodedSize() == 0;
    }

    // PersistitIndexRowBuffer interface

    public void tableBitmap(long bitmap)
    {
        value.put(bitmap);
    }

    // For table index rows
    public PersistitIndexRowBuffer(Key key)
    {
        key.clear();
        this.keyAppender = PersistitKeyAppender.create(key);
        this.value = null;
    }

    // For group index rows
    public PersistitIndexRowBuffer(Key key, Value value)
    {
        key.clear();
        this.keyAppender = PersistitKeyAppender.create(key);
        value.clear();
        this.value = value;
    }

    // For use by subclasses

    protected void attach(PersistitKeyValueSource source, int position, AkType type)
    {
        source.attach(keyAppender.key(), position, type);
    }

    protected void attach(PersistitKeyPValueSource source, int position, PUnderlying type)
    {
        source.attach(keyAppender.key(), position, type);
    }

    protected void copyFrom(Exchange exchange) throws PersistitException
    {
        exchange.getKey().copyTo(keyAppender.key());
    }

    protected void constructHKeyFromIndexKey(Key hKey, IndexToHKey indexToHKey)
    {
        Key indexRowKey = keyAppender.key();
        hKey.clear();
        for(int i = 0; i < indexToHKey.getLength(); ++i) {
            if(indexToHKey.isOrdinal(i)) {
                hKey.append(indexToHKey.getOrdinal(i));
            }
            else {
                int depth = indexToHKey.getIndexRowPosition(i);
                if (depth < 0 || depth > indexRowKey.getDepth()) {
                    throw new IllegalStateException(
                        "IndexKey too shallow - requires depth=" + depth
                        + ": " + indexRowKey);
                }
                PersistitKey.appendFieldFromKey(hKey, indexRowKey, depth);
            }
        }
    }

    // Object state

    private final PersistitKeyAppender keyAppender;
    private final Value value;
}
