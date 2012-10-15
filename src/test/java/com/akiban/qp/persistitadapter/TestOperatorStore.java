/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.GroupIndex;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.AisHolder;
import com.akiban.server.store.SchemaManager;
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
    public TestOperatorStore(AisHolder aisHolder, TreeService treeService, SchemaManager schemaManager) {
        super(aisHolder, treeService, null, schemaManager);
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
