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
public interface VStore {
    public boolean isDeltaMonitorActivated();
    public int getDeltaThreshold();
    public VBulkLoader getVStore();
    public VMeta getVMeta();
    public void setVMeta(VMeta meta);
    public String getDataPath();
    public DecisionEngine getDecisionEngine();
    public DeltaMonitor getDeltaMonitor();
}
