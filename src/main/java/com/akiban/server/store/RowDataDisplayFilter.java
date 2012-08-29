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

package com.akiban.server.store;

import com.persistit.Exchange;
import com.persistit.Management.DisplayFilter;

class RowDataDisplayFilter implements DisplayFilter {
    private DisplayFilter defaultFilter;

    public RowDataDisplayFilter(final DisplayFilter filter) {
        this.defaultFilter = filter;
    }

    public String toKeyDisplayString(final Exchange exchange) {
        return defaultFilter.toKeyDisplayString(exchange);
    }

    public String toValueDisplayString(final Exchange exchange) {
        final String treeName = exchange.getTree().getName();

        // TODO: UPDATE FOR PROTOBUF
        /*
        if (treeName.equals(TreeService.SCHEMA_TREE_NAME)) {
            try {
                final Key key = exchange.getKey();
                if(key.decodeString().equals("byAIS")) {
                    byte[] storedAIS = exchange.fetch().getValue().getByteArray();
                    GrowableByteBuffer buffer = GrowableByteBuffer.wrap(storedAIS);
                    AkibanInformationSchema ais = new Reader(new MessageSource(buffer)).load();
                    return CSVTarget.toString(ais);
                }
            } catch (Exception e) {
                // fall through and attempt to use default display filter
            }
        }*/
        return defaultFilter.toValueDisplayString(exchange);
    }
}
