
package com.akiban.sql.server;

import com.akiban.server.types3.Types3Switch;
import com.akiban.sql.optimizer.OperatorCompiler;

public abstract class ServerOperatorCompiler extends OperatorCompiler
{
    protected ServerOperatorCompiler() {
    }

    protected void initServer(ServerSession server) {
        boolean usePValues = server.getBooleanProperty("newtypes", Types3Switch.DEFAULT);
        // the following is racy, but everything about the Types3Switch is
        if (usePValues != Types3Switch.ON)
            Types3Switch.ON = usePValues;
        initProperties(server.getCompilerProperties());
        initAIS(server.getAIS(), server.getDefaultSchemaName());
        initParser(server.getParser());
        initFunctionsRegistry(server.functionsRegistry());
        initCostEstimator(server.costEstimator(this, server.getTreeService()), usePValues);
        if (usePValues)
            initT3Registry(server.t3RegistryService());
        
        server.getBinderContext().setBinderAndTypeComputer(binder, typeComputer);

        server.setAttribute("compiler", this);
    }

}
