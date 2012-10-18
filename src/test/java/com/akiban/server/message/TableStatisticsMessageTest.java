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

package com.akiban.server.message;

import static org.junit.Assert.assertEquals;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.util.GrowableByteBuffer;
import org.junit.Test;

import com.akiban.server.TableStatistics;

public class TableStatisticsMessageTest {

    @Test
    public void testTableStatisticsResponseMessageSerialization()
            throws Exception {
        String[] DDL = new String[] {
            "create table test(",
            "    a int, ",
            "    b int, ",
            "    c smallint, ",
            "    unique(b));"
        };
        SchemaFactory schemaFactory = new SchemaFactory("schema");
        AkibanInformationSchema ais = schemaFactory.aisWithRowDefs(DDL);
        RowDef rowDef = ais.getTable("schema", "test").rowDef();
        final TableStatistics ts = new TableStatistics(123);
        ts.setAutoIncrementValue(999);
        ts.setBlockSize(8192);
        ts.setCreationTime(88888);
        ts.setMeanRecordLength(200);
        ts.setRowCount(12345678);
        ts.setUpdateTime(99999);
        final TableStatistics.Histogram hs = new TableStatistics.Histogram(456);
        for (int i = 1; i <= 10; i++) {
            final RowData rowData = new RowData(new byte[256]);
            rowData.createRow(rowDef, new Object[] { null, i, null });
            final TableStatistics.HistogramSample sample = new TableStatistics.HistogramSample(
                    rowData, i * 7);
            hs.addSample(sample);
        }
        ts.addHistogram(hs);

        final GrowableByteBuffer payload = new GrowableByteBuffer(65536);
        final GetTableStatisticsResponse message = new GetTableStatisticsResponse(
                123, ts);

        message.write(payload);
        payload.flip();

        final GetTableStatisticsResponse message2 = new GetTableStatisticsResponse();
        message2.read(payload);
        assertEquals(999, ts.getAutoIncrementValue());
        assertEquals(8192, ts.getBlockSize());
        assertEquals(88888, ts.getCreationTime());
        assertEquals(200, ts.getMeanRecordLength());
        assertEquals(12345678, ts.getRowCount());
        assertEquals(99999, ts.getUpdateTime());
        assertEquals(123, ts.getRowDefId());
        assertEquals(1, ts.getHistogramList().size());
        assertEquals(10, ts.getHistogramList().get(0).getHistogramSamples()
                .size());
        assertEquals(payload.position(), payload.limit());
    }
}
