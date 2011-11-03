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
package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.GroupIndex;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.AisHolder;
import com.google.inject.Inject;
import com.persistit.Key;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TestOperatorStore extends OperatorStore {

    // TestOperatorStore interface

    public List<String> getAndClearHookStrings() {
        return hook.getAndClear();
    }

    public void clearHookStrings() {
        hook.getAndClear();
    }

    @Inject
    public TestOperatorStore(AisHolder aisHolder, TreeService treeService) {
        super(aisHolder, treeService, null);
    }

    // service overrides

    @Override
    public void start() {
        super.start();
        OperatorStoreGIHandler.setGiHandlerHook(hook);
    }

    @Override
    public void stop() {
        OperatorStoreGIHandler.setGiHandlerHook(null);
        clearHookStrings();
        super.stop();
    }

    // OperatorStore overrides

    @Override
    protected Collection<GroupIndex> optionallyOrderGroupIndexes(Collection<GroupIndex> groupIndexes) {
        List<GroupIndex> ordered = new ArrayList<GroupIndex>(groupIndexes);
        Collections.sort(ordered, GI_COMPARATOR);
        return ordered;
    }

    // object state

    private final OperatorStoreGIHandlerHook hook = new OperatorStoreGIHandlerHook();

    // consts

    private static final Comparator<GroupIndex> GI_COMPARATOR = new Comparator<GroupIndex>() {
        @Override
        public int compare(GroupIndex o1, GroupIndex o2) {
            String o1Name = o1.getIndexName().getName();
            String o2Name = o2.getIndexName().getName();
            return o1Name.compareTo(o2Name);
        }
    };

    // nested classes
    private static class OperatorStoreGIHandlerHook implements OperatorStoreGIHandler.GIHandlerHook {

        public List<String> getAndClear() {
            synchronized (strings) {
                List<String> ret = new ArrayList<String>(strings);
                strings.clear();
                return ret;
            }
        }

        @Override
        public void storeHook(GroupIndex groupIndex, Key key, Object value) {
            see("STORE to %s %s => %s", groupIndex.getIndexName().getName(), key, value);
        }

        @Override
        public void removeHook(GroupIndex groupIndex, Key key) {
            see("REMOVE from %s %s", groupIndex.getIndexName().getName(), key);
        }

        private void see(String format, Object... args) {
            strings.add(String.format(format, args));
        }

        private final List<String> strings = Collections.synchronizedList(new ArrayList<String>());
    }
}
