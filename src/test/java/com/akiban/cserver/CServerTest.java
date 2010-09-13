package com.akiban.cserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.management.ManagementFactory;

import javax.management.ObjectName;

import com.akiban.cserver.manage.MXBeanManager;
import com.akiban.cserver.message.DMLRequest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Types;
import com.akiban.cserver.manage.ManageMXBean;
import com.akiban.cserver.message.GetAutoIncrementValueRequest;
import com.akiban.cserver.message.GetAutoIncrementValueResponse;
import com.akiban.cserver.message.GetTableStatisticsRequest;
import com.akiban.cserver.message.GetTableStatisticsResponse;
import com.akiban.cserver.message.WriteRowRequest;
import com.akiban.cserver.message.WriteRowResponse;
import com.akiban.message.AkibaConnection;
import com.akiban.message.AkibaConnectionImpl;
import com.akiban.message.Message;
import com.akiban.message.MessageRegistryBase;
import com.akiban.network.AkibaNetworkHandler;
import com.akiban.network.NetworkHandlerFactory;

public class CServerTest implements CServerConstants {

    private final static File DATA_PATH = new File("/tmp/data");

    private final static String DDL_FILE_NAME = "src/test/resources/drupal_ddl_test.ddl";

    private final static RowDef ROW_DEF = RowDef
            .createRowDef(1234,
                    new FieldDef[] { new FieldDef("a", Types.INT),
                            new FieldDef("b", Types.INT),
                            new FieldDef("c", Types.INT) }, "test",
                    "group_table_test", new int[] { 0 });

    private static AkibaNetworkHandler networkHandler;

    private static AkibaConnection connection;

    private static CServer cserver;
    
    private static ObjectName mxbeanName;


    @BeforeClass
    public static void setUpSuite() throws Exception {
        CServerUtil.cleanUpDirectory(DATA_PATH);
        MessageRegistryBase.reset();
        cserver = new CServer(false);
        cserver.setProperty("cserver.fixed", "true");
        cserver.setProperty("cserver.datapath", DATA_PATH.getPath());
        cserver.start();
        ROW_DEF.setRowType(RowType.ROOT);
        ROW_DEF.setGroupRowDefId(ROW_DEF.getRowDefId());
        ROW_DEF.setUserTableRowDefs(new RowDef[] { ROW_DEF });

        cserver.getRowDefCache().putRowDef(ROW_DEF);

        networkHandler = NetworkHandlerFactory.getHandler("localhost", "5140",
                null);
        connection = AkibaConnectionImpl.createConnection(networkHandler);
        mxbeanName = new ObjectName(ManageMXBean.MANAGE_BEAN_NAME);
    }

    @AfterClass
    public static void tearDownSuite() throws Exception {
        if (networkHandler != null) {
            networkHandler.disconnectWorker();
        }
        NetworkHandlerFactory.closeNetwork();
        cserver.stop();
    }

    @Before
    public void setUp() throws Exception {
        cserver.getStore().truncateTable(ROW_DEF.getRowDefId());
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testWriteRowResponse() throws Exception {
        final WriteRowRequest request = createWriteRowRequest();
        setAisGeneration(request);
        final WriteRowResponse response = (WriteRowResponse) connection
                .sendAndReceive(request);
        assertEquals(1, response.getResultCode());
    }

    @Test
    public void testGetAutoIncrementRequestResponse() throws Exception {
        DMLRequest request;
        Message response;

        startMessageCapture();
        
        request = new GetAutoIncrementValueRequest(ROW_DEF.getRowDefId());
        setAisGeneration(request);

        response = connection.sendAndReceive(request);
        assertTrue(response instanceof GetAutoIncrementValueResponse);
        assertEquals(-1, ((GetAutoIncrementValueResponse) response).getValue());

        request = createWriteRowRequest();
        setAisGeneration(request);
        response = connection.sendAndReceive(request);
        assertEquals(OK, ((WriteRowResponse) response).getResultCode());

        request = new GetAutoIncrementValueRequest(ROW_DEF.getRowDefId());
        setAisGeneration(request);
        response = connection.sendAndReceive(request);
        assertTrue(response instanceof GetAutoIncrementValueResponse);
        assertEquals(1, ((GetAutoIncrementValueResponse) response).getValue());

        displayCapturedMessages();
        stopMessageCapture();
    }

    @Test
    public void testGetTableStatisticsRequestResponse() throws Exception {
        DMLRequest request;
        Message response;

        request = createWriteRowRequest();
        setAisGeneration(request);
        response = connection.sendAndReceive(request);
        assertEquals(1, ((WriteRowResponse) response).getResultCode());

        request = new GetTableStatisticsRequest(ROW_DEF.getRowDefId(), (byte) 0);
        setAisGeneration(request);
        response = connection.sendAndReceive(request);

        GetTableStatisticsResponse gtsr = (GetTableStatisticsResponse) response;

        final TableStatistics ts = gtsr.getTableStatistics();
        assertEquals(1, ts.getRowCount());
        // assertEquals(1, ts.getAutoIncrementValue());
        assertTrue(ts.getCreationTime() - System.currentTimeMillis() < 10000);
        assertEquals(8192, ts.getBlockSize());
        // TODO - change when the histogram list is populated
        assertEquals(0, ts.getHistogramList().size());
    }

    /**
     * Make sure we can load the Drupal schema via DDLSource. Padraig
     * encountered a problem with an incorrectly written schema file. Leaving
     * this test here for now in case we need to test newer versions.
     */
    @Test
    public void testDrupalSchema() throws Exception {
        final AkibaInformationSchema ais = new DDLSource()
                .buildAIS(DDL_FILE_NAME);
        final CServer cserver = new CServer(false);
        cserver.getRowDefCache().setAIS(ais);
    }

    private void setAisGeneration(DMLRequest request) {
        try {
            request.setAisGeneration(MXBeanManager.getSchemaManager().getSchemaID().getGeneration());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private WriteRowRequest createWriteRowRequest() {
        final RowData rowData = new RowData(new byte[64]);
        rowData.createRow(ROW_DEF, new Object[] { 1, 2, 3 });
        final WriteRowRequest request = new WriteRowRequest();
        request.setRowData(rowData);
        return request;
    }

    private void startMessageCapture() throws Exception {
        ManagementFactory.getPlatformMBeanServer().invoke(mxbeanName, "startMessageCapture", new Object[0], new String[0]);
    }

    private void stopMessageCapture() throws Exception {
        ManagementFactory.getPlatformMBeanServer().invoke(mxbeanName, "stopMessageCapture", new Object[0], new String[0]);
    }
    
    private void displayCapturedMessages() throws Exception {
        ManagementFactory.getPlatformMBeanServer().invoke(mxbeanName, "displayCapturedMessages", new Object[0], new String[0]);
    }
    
    
}
