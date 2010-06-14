package com.akiban.cserver.message;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.akiban.ais.model.Types;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.IndexDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.TableStatistics;
import com.persistit.Key;
import com.persistit.Persistit;

public class TableStatisticsMessageTest {

	@Test
	public void testTableStatisticsResponseMessageSerialization() throws Exception {
		final RowDef rowDef = new RowDef(123, new FieldDef[] {
				new FieldDef("a", Types.TINYINT),
				new FieldDef("b", Types.TINYINT),
				new FieldDef("c", Types.SMALLINT), });
		rowDef.setIndexDefs(new IndexDef[] { new IndexDef("x", rowDef, "_x",
				456, new int[] { 1 }, false, true) });

		final TableStatistics ts = new TableStatistics(123);
		ts.setAutoIncrementValue(999);
		ts.setBlockSize(8192);
		ts.setCreationTime(88888);
		ts.setMeanRecordLength(200);
		ts.setRowCount(12345678);
		ts.setUpdateTime(99999);
		final TableStatistics.Histogram hs = new TableStatistics.Histogram(456);
		final Key key = new Key((Persistit) null);
		for (int i = 1; i <= 10; i++) {
			final RowData rowData = new RowData(new byte[256]);
			rowData.createRow(rowDef, new Object[] { null, i, null });
			final TableStatistics.HistogramSample sample = new TableStatistics.HistogramSample(
					rowData, i * 7);
			hs.addSample(sample);
		}
		ts.addHistogram(hs);
		
		
		final ByteBuffer payload = ByteBuffer.allocate(65536);
		final GetTableStatisticsResponse message = new GetTableStatisticsResponse(123, 100, ts);
		
		message.write(payload);
		payload.flip();
		
		final GetTableStatisticsResponse message2 = new GetTableStatisticsResponse();
		message2.read(payload);
		assertEquals(999, ts.getAutoIncrementValue());
		assertEquals(8192,ts.getBlockSize());
		assertEquals(88888, ts.getCreationTime());
		assertEquals(200, ts.getMeanRecordLength());
		assertEquals(12345678, ts.getRowCount());
		assertEquals(99999, ts.getUpdateTime());
		assertEquals(123, ts.getRowDefId());
		assertEquals(1, ts.getHistogramList().size());
		assertEquals(10, ts.getHistogramList().get(0).getHistogramSamples().size());
		assertEquals(payload.position(), payload.limit());
	}
}
