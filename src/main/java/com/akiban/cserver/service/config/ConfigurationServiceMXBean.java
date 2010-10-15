package com.akiban.cserver.service.config;

import com.akiban.cserver.service.MxService;

import java.util.Set;

public interface ConfigurationServiceMXBean extends MxService
{
    Set<Property> getProperties();
}
