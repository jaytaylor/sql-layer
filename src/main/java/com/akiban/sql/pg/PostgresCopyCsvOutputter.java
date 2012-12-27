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

package com.akiban.sql.pg;

import com.akiban.sql.server.ServerValueEncoder;
import com.akiban.qp.row.Row;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.IOException;

public class PostgresCopyCsvOutputter extends PostgresOutputter<Row>
{
    private CsvFormat format;

    public PostgresCopyCsvOutputter(PostgresQueryContext context,
                                    PostgresDMLStatement statement,
                                    CsvFormat format) {
        super(context, statement);
        this.format = format;
        encoder = new ServerValueEncoder(format.getEncoding(),
                                         new QuotingByteArrayOutputStream());
    }

    @Override
    public void output(Row row, boolean usePVals) throws IOException {
        messenger.beginMessage(PostgresMessages.COPY_DATA_TYPE.code());
        output(row, messenger.getRawOutput(), usePVals);
        messenger.sendMessage();
    }

    @Override
    public void beforeData() throws IOException {
        messenger.beginMessage(PostgresMessages.COPY_OUT_RESPONSE_TYPE.code());
        messenger.write(0);
        messenger.writeShort(ncols);
        for (int i = 0; i < ncols; i++) {
            assert !context.isColumnBinary(i);
            messenger.writeShort(0);
        }
        messenger.sendMessage();
        if (format.getHeadings() != null) {
            messenger.beginMessage(PostgresMessages.COPY_DATA_TYPE.code());
            outputHeadings(messenger.getRawOutput());
            messenger.sendMessage();
        }
    }

    @Override
    public void afterData() throws IOException {
        messenger.beginMessage(PostgresMessages.COPY_DONE_TYPE.code());
        messenger.sendMessage();
    }

    public void output(Row row, OutputStream outputStream, boolean usePVals) 
            throws IOException {
        for (int i = 0; i < ncols; i++) {
            if (i > 0) outputStream.write(format.getDelimiterByte());
            PostgresType type = columnTypes.get(i);
            boolean binary = context.isColumnBinary(i);
            QuotingByteArrayOutputStream bytes;
            if (usePVals) bytes = (QuotingByteArrayOutputStream)encoder.encodePValue(row.pvalue(i), type, binary);
            else bytes = (QuotingByteArrayOutputStream)encoder.encodeValue(row.eval(i), type, binary);
            if (bytes != null) {
                bytes.quote(format);
                bytes.writeTo(outputStream);
            }
            else {
                outputStream.write(format.getNullBytes());
            }
        }
        outputStream.write(format.getRecordEndBytes());
    }

    public void outputHeadings(OutputStream outputStream) throws IOException {
        for (int i = 0; i < ncols; i++) {
            if (i > 0) outputStream.write(format.getDelimiterByte());
            outputStream.write(format.getHeadingBytes(i));
        }
        outputStream.write(format.getRecordEndBytes());
    }

    static class QuotingByteArrayOutputStream extends ByteArrayOutputStream {
        public void quote(CsvFormat format) {
            if (needsQuoting(format.getRequiresQuoting())) {
                for (int i = 0; i < count; i++) {
                    int b = buf[i] & 0xFF;
                    if ((b == format.getQuoteByte()) ||
                        (b == format.getEscapeByte())) {
                        insert(i, format.getEscapeByte());
                        i++;
                    }
                }
                insert(0, format.getQuoteByte());
                insert(count, format.getQuoteByte());
            }
        }

        private boolean needsQuoting(byte[] quotable) {
            for (int i = 0; i < count; i++) {
                byte b = buf[i];
                for (int j = 0; j < quotable.length; j++) {
                    if (b == quotable[j]) {
                        return true;
                    }
                }
            }
            return false;
        }

        private void insert(int pos, int b) {
            write(b);           // for private ensureCapacity.
            System.arraycopy(buf, pos, buf, pos+1, count-pos-1);
            buf[pos] = (byte)b;
        }
    }
 
}
