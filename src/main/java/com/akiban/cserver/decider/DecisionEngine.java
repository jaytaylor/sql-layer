/**
 * 
 */
package com.akiban.cserver.decider;

import com.akiban.cserver.CServerConfig;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.message.ScanRowsRequest;

/**
 * @author percent
 *
 */
public class DecisionEngine {
    public enum Strategy { HStore, VStore, SelectivityAndVolume}
    
    public static DecisionEngine createDecisionEngine(CServerConfig config) {
        final String decisionEngineConfig = config.property(DECISION_ENGINE);
        DecisionEngine ret = new DecisionEngine(); 
        if(decisionEngineConfig != null) {
            if(decisionEngineConfig.equals("hstore")) {
                ret.decider = new Monadic(Decider.RowCollectorType.PersistitRowCollector);
                ret.type = Strategy.HStore;
            } else if(decisionEngineConfig.equals("vstore")) {
                ret.decider = new Monadic(Decider.RowCollectorType.VCollector);
                ret.type = Strategy.VStore;
            } else {
                assert false;
            }
        } else {
            ret.decider = new Monadic(Decider.RowCollectorType.PersistitRowCollector);
            ret.type = Strategy.HStore;
        }
        
       assert ret != null && ret.decider != null;
       return ret;
    }
    
    public Decider.RowCollectorType makeDecision(ScanRowsRequest request, RowDef rowDef) {
        assert decider != null && request != null;
        return decider.decide(request, rowDef);
    }

    public Strategy getStrategy() {
        return type;
    }

    private DecisionEngine() {
        decider = null;
    }
    
    private static final String DECISION_ENGINE = "cserver.decision_engine";
    private Decider decider;
    private Strategy type;
}
