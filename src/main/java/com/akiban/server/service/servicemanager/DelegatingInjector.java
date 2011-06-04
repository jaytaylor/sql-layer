/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.service.servicemanager;

import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.MembersInjector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;

import java.util.List;
import java.util.Map;

public class DelegatingInjector implements Injector {

    @Override
    public void injectMembers(Object instance) {
        delegate.injectMembers(instance);
    }

    @Override
    public <T> MembersInjector<T> getMembersInjector(TypeLiteral<T> typeLiteral) {
        return delegate.getMembersInjector(typeLiteral);
    }

    @Override
    public <T> MembersInjector<T> getMembersInjector(Class<T> type) {
        return delegate.getMembersInjector(type);
    }

    @Override
    public Map<Key<?>, Binding<?>> getBindings() {
        return delegate.getBindings();
    }

    @Override
    public <T> Binding<T> getBinding(Key<T> key) {
        return delegate.getBinding(key);
    }

    @Override
    public <T> Binding<T> getBinding(Class<T> type) {
        return delegate.getBinding(type);
    }

    @Override
    public <T> List<Binding<T>> findBindingsByType(TypeLiteral<T> type) {
        return delegate.findBindingsByType(type);
    }

    @Override
    public <T> Provider<T> getProvider(Key<T> key) {
        return delegate.getProvider(key);
    }

    @Override
    public <T> Provider<T> getProvider(Class<T> type) {
        return delegate.getProvider(type);
    }

    @Override
    public <T> T getInstance(Key<T> key) {
        return delegate.getInstance(key);
    }

    @Override
    public <T> T getInstance(Class<T> type) {
        return delegate.getInstance(type);
    }

    @Override
    public Injector getParent() {
        return delegate.getParent();
    }

    @Override
    public Injector createChildInjector(Iterable<? extends Module> modules) {
        return delegate.createChildInjector(modules);
    }

    @Override
    public Injector createChildInjector(Module... modules) {
        return delegate.createChildInjector(modules);
    }

    public DelegatingInjector(Injector delegate) {
        this.delegate = delegate;
    }

    protected Injector delgate() {
        return delegate;
    }

    private final Injector delegate;
}
