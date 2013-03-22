
package com.akiban.server.test.it.store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MultiVolumeStoreIT extends ITBase {
    private final static String DEFAULT_SCHEMA = "default_schema";
    private final static String TEST1_SCHEMA = "test1";
    private final static String TEST2_SCHEMA = "test2";

    @Override
    protected Map<String, String> startupConfigProperties() {
        // Set up multi-volume treespace policy so we can be sure schema is properly distributed.
        final Map<String, String> properties = new HashMap<>();
        properties.put("akserver.treespace.1",
                                    "test2/_akiban_customer:${datapath}/${schema}_customer.v0,create,pageSize:"+
                                    "${buffersize},initialSize:10K,extensionSize:1K,maximumSize:10G");
        properties.put("akserver.treespace.2",
                                    "test*:${datapath}/${schema}.v0,create,pageSize:"+
                                    "${buffersize},initialSize:10K,extensionSize:1K,maximumSize:10G");
        return properties;
    }

    @Before
    public void setUp() throws Exception {
        DataDictionaryDDL.createTables(session(), ddl(), DEFAULT_SCHEMA);
        DataDictionaryDDL.createTables(session(), ddl(), TEST1_SCHEMA);
        DataDictionaryDDL.createTables(session(), ddl(), TEST2_SCHEMA);
        updateAISGeneration();
    }

    @Test
    public void insertCOIrows() throws Exception {
        final RowDef defaultRowDef = getRowDef(DEFAULT_SCHEMA, "customer");
        final RowDef test1RowDef = getRowDef(TEST1_SCHEMA, "customer");
        final RowDef test2RowDef = getRowDef(TEST2_SCHEMA, "customer");

        final RowData rowData = new RowData(new byte[1024]);
        final Object[] values = new Object[] { 1, "Acme Manufacturing" };
        final LegacyRowWrapper wrapper = new LegacyRowWrapper();

        rowData.createRow(defaultRowDef, values);
        wrapper.setRowData(rowData);
        dml().writeRow(session(),  wrapper);

        rowData.createRow(test1RowDef, values);
        wrapper.setRowData(rowData);
        dml().writeRow(session(), wrapper);

        for (int cid = 1; cid <= 5; cid++) {
            values[0] = cid;
            rowData.createRow(test2RowDef, values);
            wrapper.setRowData(rowData);
            dml().writeRow(session(), wrapper);
        }

        List<RowData> rows;
        rows = scanFull(scanAllRequest(defaultRowDef.getRowDefId()));
        assertEquals("rows in default", 1, rows.size());
        rows = scanFull(scanAllRequest(test1RowDef.getRowDefId()));
        assertEquals("rows in test1", 1, rows.size());
        rows = scanFull(scanAllRequest(test2RowDef.getRowDefId()));
        assertEquals("rows in test2", 5, rows.size());
    }
}
