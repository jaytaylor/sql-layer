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

package com.akiban.server.store;

import com.akiban.server.AkServerUtil;
import com.akiban.server.rowdata.CorruptRowDataException;
import com.akiban.server.rowdata.RowData;
import com.persistit.Value;
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.ValueDisplayer;
import com.persistit.encoding.ValueRenderer;
import com.persistit.exception.ConversionException;

public class RowDataValueCoder implements ValueDisplayer, ValueRenderer {
    final static int INITIAL_BUFFER_SIZE = 1024;

    RowDataValueCoder() {
    }

    @Override
    public void put(Value value, Object object, CoderContext context) throws ConversionException {
        final RowData rowData = (RowData) object;
        final int start = rowData.getInnerStart();
        final int size = rowData.getInnerSize();
        value.ensureFit(size);
        final int at = value.getEncodedSize();
        System.arraycopy(rowData.getBytes(), start, value.getEncodedBytes(), at, size);
        value.setEncodedSize(at + size);
    }

    @Override
    public Object get(Value value, Class<?> clazz, CoderContext context) throws ConversionException {
        final RowData rowData = new RowData(new byte[INITIAL_BUFFER_SIZE]);
        render(value, rowData, RowData.class, null);
        return rowData;
    }

    @Override
    public void render(Value value, Object target, Class<?> clazz, CoderContext context) throws ConversionException {
        final RowData rowData = (RowData) target;
        final int at = value.getCursor();
        final int end = value.getEncodedSize();
        final int size = end - at;

        final int rowDataSize = size + RowData.ENVELOPE_SIZE;
        final byte[] valueBytes = value.getEncodedBytes();
        byte[] rowDataBytes = rowData.getBytes();

        if (rowDataSize < RowData.MINIMUM_RECORD_LENGTH || rowDataSize > RowData.MAXIMUM_RECORD_LENGTH) {
            throw new CorruptRowDataException("RowData is too short or too long: " + rowDataSize);
        }

        if (rowDataSize > rowDataBytes.length) {
            rowDataBytes = new byte[rowDataSize + INITIAL_BUFFER_SIZE];
            rowData.reset(rowDataBytes);
        }

        //
        // Assemble the Row in a byte array 
        //
        AkServerUtil.putInt(rowDataBytes, RowData.O_LENGTH_A, rowDataSize);
        AkServerUtil.putShort(rowDataBytes, RowData.O_SIGNATURE_A, RowData.SIGNATURE_A);
        System.arraycopy(valueBytes, at, rowDataBytes, RowData.O_FIELD_COUNT, size);
        AkServerUtil.putShort(rowDataBytes, RowData.O_SIGNATURE_B + rowDataSize, RowData.SIGNATURE_B);
        AkServerUtil.putInt(rowDataBytes, RowData.O_LENGTH_B + rowDataSize, rowDataSize);
        rowData.prepareRow(0);

        value.setCursor(end);
    }

    @Override
    public void display(Value value, StringBuilder target, Class<?> clazz, CoderContext context) throws ConversionException {
        Object object = get(value, clazz, context);
        if (object instanceof RowData) {
            final RowData rowData = (RowData) object;
            target.append(rowData.toString());
        } else {
            target.append(object);
        }
    }
}
