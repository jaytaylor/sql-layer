
package com.akiban.server.service.servicemanager.configuration;

import com.google.inject.Module;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public final class DefaultServiceConfigurationHandler implements ServiceConfigurationHandler {

    // YamlConfigurationStrategy interface

    @Override
    public void bind(String interfaceName, String implementingClassName, ClassLoader classLoader) {
        builder.bind(interfaceName, implementingClassName, classLoader);
    }

    @Override
    public void bindModules(List<Module> modules) {
        if (this.modules == null)
            this.modules = new ArrayList<>(modules.size());
        this.modules.addAll(modules);
    }

    @Override
    public void lock(String interfaceName) {
        builder.lock(interfaceName);
    }

    @Override
    public void require(String interfaceName) {
        builder.markDirectlyRequired(interfaceName);
    }

    @Override
    public void mustBeLocked(String interfaceName) {
        builder.mustBeLocked(interfaceName);
    }

    @Override
    public void mustBeBound(String interfaceName) {
        builder.mustBeBound(interfaceName);
    }

    @Override
    public void prioritize(String interfaceName) {
        builder.prioritize(interfaceName);
    }

    @Override
    public void sectionEnd() {
        builder.markSectionEnd();
    }

    @Override
    public void unrecognizedCommand(String where, Object command, String message) {
        throw new ServiceConfigurationException(
                String.format("unrecognized command at %s: %s (%s)",
                        where,
                        message,
                        command
                )
        );
    }

    @Override
    public void bindModulesError(String where, Object command, String message) {
        throw new ServiceConfigurationException(
                String.format("bind-modules error at %s: %s (%s)",
                        where,
                        message,
                        command
                )
        );
    }

    // DefaultServiceConfigurationHandler interface

    public Collection<? extends Module> getModules() {
        Collection<Module> internal = modules == null ? Collections.<Module>emptyList() : modules;
        return Collections.unmodifiableCollection(internal);
    }

    public Collection<ServiceBinding> serviceBindings(boolean strict) {
        return builder.getAllBindings(strict);
    }

    public List<String> priorities() {
        return builder.getPriorities();
    }

    // object state
    private final ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
    private Collection<Module> modules = null;
}
