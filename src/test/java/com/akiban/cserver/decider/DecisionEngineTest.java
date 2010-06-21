/**
 * 
 */
package com.akiban.cserver.decider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.CServerConfig;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.message.ScanRowsRequest;

/**
 * @author percent
 * 
 */

public class DecisionEngineTest {

    private final static String VCOLLECTOR_DDL = "src/test/resources/vcollector_test-1.ddl";
    private final static String DECISION_HACK_DDL = "src/test/resources/decision-test-schema.ddl";
    private final static String TOY_TEST_DDL = "src/test/resources/toy_schema.ddl";

    private final static String MANY_DDL = "src/test/resources/many_columns.ddl";
    private final static String VCOLLECTOR_TEST_DATADIR = "vcollector_test_data/";
    private RowDefCache rowDefCache;
    private AkibaInformationSchema ais;

    public void setupDatabase(String ddl) throws Exception {

        rowDefCache = new RowDefCache();

        ais = null;
        try {
            ais = new DDLSource().buildAIS(ddl);
        } catch (Exception e1) {
            e1.printStackTrace();
            throw e1;
            // return;
        }
        rowDefCache.setAIS(ais);
    }

    @Test
    public void testTemporaryHack() throws Exception {
        setupDatabase(DECISION_HACK_DDL);
        RowDef rowDef = null;
        CServerConfig config = new CServerConfig();
        config.getProperties().put("cserver.decision_engine", "vstore");
        DecisionEngine de = DecisionEngine.createDecisionEngine(config);
        ScanRowsRequest request = new ScanRowsRequest();
        Iterator<RowDef> i = rowDefCache.getRowDefs().iterator();

        while (i.hasNext()) {
            rowDef = i.next();
            if (rowDef.getSchemaName().equals("toy_test1")) {
                break;
            }

        }
        assertTrue(rowDef != null);

        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));

        setupDatabase(VCOLLECTOR_DDL);
        i = rowDefCache.getRowDefs().iterator();

        while (i.hasNext()) {
            rowDef = i.next();
            if (rowDef.getSchemaName().equals("toy_test")) {
                break;
            }

        }

        assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(
                request, rowDef));
        assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(
                request, rowDef));
        assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(
                request, rowDef));
        assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(
                request, rowDef));
        assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(
                request, rowDef));

    }

    @Test
    public void testGetStrategy() {
        CServerConfig config = new CServerConfig();
        DecisionEngine de = DecisionEngine.createDecisionEngine(config);
        assertEquals(DecisionEngine.Strategy.HStore, de.getStrategy());

        config.getProperties().put("cserver.decision_engine", "vstore");
        de = DecisionEngine.createDecisionEngine(config);
        assertEquals(DecisionEngine.Strategy.VStore, de.getStrategy());

        config.getProperties().put("cserver.decision_engine", "hstore");
        de = DecisionEngine.createDecisionEngine(config);
        assertEquals(DecisionEngine.Strategy.HStore, de.getStrategy());

        config.getProperties().put("cserver.decision_engine", "fuck-show");
        boolean error = false;
        try {
            de = DecisionEngine.createDecisionEngine(config);
        } catch (AssertionError e) {
            error = true;
        }
        assertTrue(error);

    }

    @Test
    public void testMakeDecision() {

        CServerConfig config = new CServerConfig();
        DecisionEngine de = DecisionEngine.createDecisionEngine(config);
        ScanRowsRequest request = new ScanRowsRequest();
        RowDef rowDef = null;
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));

        config.getProperties().put("cserver.decision_engine", "vstore");
        de = DecisionEngine.createDecisionEngine(config);

        assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(
                request, rowDef));
        assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(
                request, rowDef));
        assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(
                request, rowDef));
        assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(
                request, rowDef));
        assertEquals(Decider.RowCollectorType.VCollector, de.makeDecision(
                request, rowDef));

        config.getProperties().put("cserver.decision_engine", "hstore");
        de = DecisionEngine.createDecisionEngine(config);

        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));
        assertEquals(Decider.RowCollectorType.PersistitRowCollector, de
                .makeDecision(request, rowDef));
    }
}
