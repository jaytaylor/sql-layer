
package com.akiban.server.service.servicemanager.configuration;

import com.google.inject.Module;

import java.util.List;

public interface ServiceConfigurationHandler {
    void bind(String interfaceName, String implementingClassName, ClassLoader classLoader);
    void bindModules(List<Module> modules);
    void require(String interfaceName);
    void lock(String interfaceName);
    void mustBeLocked(String interfaceName);
    void mustBeBound(String interfaceName);
    void prioritize(String interfaceName);
    void sectionEnd();
    void unrecognizedCommand(String where, Object command, String message);
    void bindModulesError(String where, Object command, String message);
}
