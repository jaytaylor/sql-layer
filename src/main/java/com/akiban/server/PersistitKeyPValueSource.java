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

package com.akiban.server;

import com.akiban.ais.model.IndexColumn;
import com.persistit.Key;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import static java.lang.Math.min;

public class PersistitKeyPValueSource implements PValueSource {

    // object state
    private Key key;
    private int depth;
    private PUnderlying pUnderlying;
    private PValue output;
    private boolean needsDecoding = true;
    
    public PersistitKeyPValueSource(PUnderlying pUnderlying) {
        this.pUnderlying = pUnderlying;
        this.output = new PValue(pUnderlying);
    }
    
    public void attach(Key key, IndexColumn indexColumn) {
        attach(key, indexColumn.getPosition(), indexColumn.getColumn().tInstance().typeClass().underlyingType());
    }

    public void attach(Key key, int depth, PUnderlying pUnderlying) {
        this.key = key;
        this.depth = depth;
        this.pUnderlying = pUnderlying;
        clear();
    }
    
    public void attach(Key key) {
        this.key = key;
        clear();
    }

    @Override
    public PUnderlying getUnderlyingType() {
        return pUnderlying;
    }

    @Override
    public boolean hasAnyValue() {
        return decode().hasAnyValue();
    }

    @Override
    public boolean hasRawValue() {
        return decode().hasRawValue();
    }

    @Override
    public boolean hasCacheValue() {
        return decode().hasCacheValue();
    }

    @Override
    public boolean isNull() {
        /*
         * No need to decode the value to detect null
         */
        if (needsDecoding) {
            key.indexTo(depth);
            return key.isNull();
        }
        return decode().isNull();
    }

    @Override
    public boolean getBoolean() {
        return decode().getBoolean();
    }

    @Override
    public boolean getBoolean(boolean defaultValue) {
        return decode().getBoolean(defaultValue);
    }

    @Override
    public byte getInt8() {
        return decode().getInt8();
    }

    @Override
    public short getInt16() {
        return decode().getInt16();
    }

    @Override
    public char getUInt16() {
        return decode().getUInt16();
    }

    @Override
    public int getInt32() {
        return decode().getInt32();
    }

    @Override
    public long getInt64() {
        return decode().getInt64();
    }

    @Override
    public float getFloat() {
        return decode().getFloat();
    }

    @Override
    public double getDouble() {
        return decode().getDouble();
    }

    @Override
    public byte[] getBytes() {
        return decode().getBytes();
    }

    @Override
    public String getString() {
        return decode().getString();
    }

    @Override
    public Object getObject() {
        return decode().getObject();
    }
    
    public int compare(PersistitKeyPValueSource that)
    {
        that.key.indexTo(that.depth);
        int thatPosition = that.key.getIndex();
        that.key.indexTo(that.depth + 1);
        int thatEnd = that.key.getIndex();
        return compareOneKeySegment(that.key.getEncodedBytes(), thatPosition, thatEnd);
    }

    public int compare(AkCollator collator, String string)
    {
        assert collator != null;
        Key thatKey = new Key(key);
        thatKey.clear();
        collator.append(thatKey, string);
        thatKey.indexTo(0);
        int thatPosition = thatKey.getIndex();
        thatKey.indexTo(1);
        int thatEnd = thatKey.getIndex();
        return compareOneKeySegment(thatKey.getEncodedBytes(), thatPosition, thatEnd);
    }

    // for use by this class
    private PValueSource decode() {
        if (needsDecoding) {
            key.indexTo(depth);
            if (key.isNull()) {
                output.putNull();
            }
            else
            {
                switch (pUnderlying) {
                    case BOOL:      output.putBool(key.decodeBoolean());        break;
                    case INT_8:     output.putInt8((byte)key.decodeLong());           break;
                    case INT_16:    output.putInt16((short)key.decodeLong());         break;
                    case UINT_16:   output.putUInt16((char)key.decodeLong());         break;
                    case INT_32:    output.putInt32((int)key.decodeLong());           break;
                    case INT_64:    output.putInt64(key.decodeLong());          break;
                    case FLOAT:     output.putFloat(key.decodeFloat());         break;
                    case DOUBLE:    output.putDouble(key.decodeDouble());       break;
                    case BYTES:     output.putBytes(key.decodeByteArray());     break;
                    case STRING:    output.putString(key.decodeString(), null);       break;
                    default: throw new UnsupportedOperationException(pUnderlying.name());
                }
            }
            needsDecoding = false;
        }
        return output;
    }
    
    private int compareOneKeySegment(byte[] thatBytes, int thatPosition, int thatEnd)
    {
        this.key.indexTo(this.depth);
        int thisPosition = this.key.getIndex();
        this.key.indexTo(this.depth + 1);
        int thisEnd = this.key.getIndex();
        byte[] thisBytes = this.key.getEncodedBytes();
        // Compare until end or mismatch
        int thisN = thisEnd - thisPosition;
        int thatN = thatEnd - thatPosition;
        int n = min(thisN, thatN);
        int end = thisPosition + n;
        while (thisPosition < end) {
            int c = thisBytes[thisPosition++] - thatBytes[thatPosition++];
            if (c != 0) {
                return c;
            }
        }
        return thisN - thatN;
    }

    private void clear() {
        needsDecoding = true;
    }
}
