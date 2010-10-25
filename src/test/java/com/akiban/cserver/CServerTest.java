package com.akiban.cserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Properties;

import javax.management.ObjectName;

import com.akiban.cserver.service.DefaultServiceManagerFactory;
import com.akiban.cserver.service.session.UnitTestServiceManagerFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.akiban.ais.model.Types;
import com.akiban.cserver.manage.ManageMXBean;
import com.akiban.cserver.message.DMLRequest;
import com.akiban.cserver.message.GetAutoIncrementValueRequest;
import com.akiban.cserver.message.GetAutoIncrementValueResponse;
import com.akiban.cserver.message.GetTableStatisticsRequest;
import com.akiban.cserver.message.GetTableStatisticsResponse;
import com.akiban.cserver.message.WriteRowRequest;
import com.akiban.cserver.message.WriteRowResponse;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.message.AkibanConnection;
import com.akiban.message.AkibanConnectionImpl;
import com.akiban.message.Message;
import com.akiban.message.MessageRegistryBase;

public class CServerTest implements CServerConstants {

    private final static File DATA_PATH = new File("/tmp/data");

    private final static String DDL_FILE_NAME = "src/test/resources/drupal_ddl_test.ddl";

    private final static RowDef ROW_DEF = RowDef
            .createRowDef(1234,
                    new FieldDef[] { new FieldDef("a", Types.INT),
                            new FieldDef("b", Types.INT),
                            new FieldDef("c", Types.INT) }, "test",
                    "group_table_test", new int[] { 0 });

    private static AkibanConnection connection;

    private static ServiceManagerImpl serviceManager;

    private static ObjectName mxbeanName;

    private static final Properties originalSystemProperties = System.getProperties();

    @BeforeClass
    public static void setUpSuite() throws Exception {
        Properties testProperties = new Properties(originalSystemProperties);
        testProperties.setProperty("akiban.admin", "NONE");
        System.setProperties(testProperties);
        CServerUtil.cleanUpDirectory(DATA_PATH);
        MessageRegistryBase.reset();
        serviceManager = (ServiceManagerImpl) new UnitTestServiceManagerFactory().serviceManager();
        serviceManager.startServices();
        ROW_DEF.setRowType(RowType.ROOT);
        ROW_DEF.setGroupRowDefId(ROW_DEF.getRowDefId());
        ROW_DEF.setUserTableRowDefs(new RowDef[] { ROW_DEF });

        serviceManager.getCServer().getStore().getRowDefCache()
                .putRowDef(ROW_DEF);

        connection = new AkibanConnectionImpl(serviceManager.getCServer()
                .host(), serviceManager.getCServer().port());
        mxbeanName = new ObjectName(ManageMXBean.MANAGE_BEAN_NAME);
    }

    @AfterClass
    public static void tearDownSuite() throws Exception {
        System.setProperties(originalSystemProperties);
        try {
            connection.close();
        } finally {
            serviceManager.stopServices();
            serviceManager = null;
        }
    }

    @Before
    public void setUp() throws Exception {
        serviceManager.getCServer().getStore()
                .truncateTable(ROW_DEF.getRowDefId());
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void qtestWriteRowResponse() throws Exception {
        final WriteRowRequest request = createWriteRowRequest();
        setAisGeneration(request);
        final Message response = connection.sendAndReceive(request);
        assertEquals("message type", WriteRowResponse.class,
                response.getClass());
    }

    @Test
    public void testGetAutoIncrementRequestResponse() throws Exception {
        DMLRequest request;
        Message response;

        request = new GetAutoIncrementValueRequest(ROW_DEF.getRowDefId());
        setAisGeneration(request);

        response = connection.sendAndReceive(request);
        assertTrue(response instanceof GetAutoIncrementValueResponse);
        assertEquals(-1, ((GetAutoIncrementValueResponse) response).getValue());

        request = createWriteRowRequest();
        setAisGeneration(request);
        response = connection.sendAndReceive(request);
        assertEquals("response type", WriteRowResponse.class,
                response.getClass());

        request = new GetAutoIncrementValueRequest(ROW_DEF.getRowDefId());
        setAisGeneration(request);
        response = connection.sendAndReceive(request);
        assertTrue(response instanceof GetAutoIncrementValueResponse);
        assertEquals(1, ((GetAutoIncrementValueResponse) response).getValue());
    }

    @Test
    public void testGetTableStatisticsRequestResponse() throws Exception {
        DMLRequest request;
        Message response;

        request = createWriteRowRequest();
        setAisGeneration(request);
        response = connection.sendAndReceive(request);
        assertEquals("message type", WriteRowResponse.class,
                response.getClass());

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

    private void setAisGeneration(DMLRequest request) {
        try {
            request.setAisGeneration(serviceManager.getCServer().getStore()
                    .getSchemaManager().getSchemaID().getGeneration());
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
}
