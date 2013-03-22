
package com.akiban.server.service.stats;

import com.akiban.util.tap.TapReport;

public interface StatisticsService {

    public void setEnabled(final String regExPattern, final boolean on); 
    public void reset(final String regExPattern);
    public TapReport[] getReport(final String regExPattern);    
    
}
