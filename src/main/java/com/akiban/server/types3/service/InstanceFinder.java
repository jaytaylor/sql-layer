
package com.akiban.server.types3.service;

import java.util.Collection;

public interface InstanceFinder {
    <T> Collection<? extends T> find(Class<? extends T> targetClass);
}
