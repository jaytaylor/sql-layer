
package com.akiban.server.service.plugins;

import com.google.common.base.Supplier;

import java.util.Collection;

public interface PluginsFinder extends Supplier<Collection<? extends Plugin>> {
}
