
package com.akiban.server.service.log4jconfig;

import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;

public final class Log4JConfigurationServiceImpl
        implements Log4JConfigurationService, Service, JmxManageable {

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo(
                "Log4JConfig",
                Log4JConfigurationMXBeanSingleton.instance(),
                Log4JConfigurationMXBean.class
        );
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
    
    @Override
    public void crash() {
    }
    
}
