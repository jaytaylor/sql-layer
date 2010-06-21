/**
 * 
 */
package com.akiban.cserver.store;

import com.akiban.cserver.decider.DecisionEngine;
import com.akiban.vstore.VMeta;

/**
 * @author percent
 * 
 */
public class VStoreTestStub implements VStore {

    public String datapath = "";
    public int threshold = 1048576;
    public VMeta meta = null;

    @Override
    public String getDataPath() {
        assert false;
        return datapath;
    }

    @Override
    public DecisionEngine getDecisionEngine() {
        assert false;
        return null;
    }

    @Override
    public DeltaMonitor getDeltaMonitor() {
        assert false;
        return null;
    }

    @Override
    public int getDeltaThreshold() {
        return threshold;
    }

    @Override
    public VMeta getVMeta() {
        assert false;
        return meta;
    }

    @Override
    public VBulkLoader getVStore() {
        assert false;
        return null;
    }

    @Override
    public boolean isDeltaMonitorActivated() {
        assert false;
        return false;
    }

    @Override
    public void setVMeta(VMeta meta) {
        assert false;
    }

}
