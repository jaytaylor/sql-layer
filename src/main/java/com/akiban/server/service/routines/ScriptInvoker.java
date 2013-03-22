
package com.akiban.server.service.routines;

public interface ScriptInvoker
{
    public String getEngineName();
    public String getFunctionName();
    public boolean isCompiled();
    public Object invoke(Object[] args);
}
