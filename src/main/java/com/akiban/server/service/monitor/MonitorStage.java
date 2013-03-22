
package com.akiban.server.service.monitor;

/** The processing stages for a query session. */
public enum MonitorStage
{
    IDLE, PARSE, OPTIMIZE, EXECUTE, COMMIT
}
