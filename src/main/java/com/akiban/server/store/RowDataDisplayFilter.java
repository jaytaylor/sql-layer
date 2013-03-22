
package com.akiban.server.store;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.protobuf.AISProtobuf;
import com.akiban.ais.protobuf.ProtobufReader;
import com.akiban.server.service.tree.TreeService;
import com.akiban.util.GrowableByteBuffer;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Management.DisplayFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RowDataDisplayFilter implements DisplayFilter {
    private static final Logger LOG = LoggerFactory.getLogger(RowDataDisplayFilter.class.getName());

    private DisplayFilter defaultFilter;

    public RowDataDisplayFilter(final DisplayFilter filter) {
        this.defaultFilter = filter;
    }

    public String toKeyDisplayString(final Exchange exchange) {
        return defaultFilter.toKeyDisplayString(exchange);
    }

    public String toValueDisplayString(final Exchange exchange) {
        final String treeName = exchange.getTree().getName();

        if (treeName.equals(TreeService.SCHEMA_TREE_NAME)) {
            try {
                final Key key = exchange.getKey();
                if(key.decodeString().equals("byPBAIS")) {
                    // Default string, will include MVV information
                    String def = defaultFilter.toValueDisplayString(exchange);
                    // New exchange (that doesn't have ignore MVV set), to get pretty-print of latest
                    Exchange ex = new Exchange(exchange);
                    byte[] storedAIS = ex.fetch().getValue().getByteArray();
                    GrowableByteBuffer buffer = GrowableByteBuffer.wrap(storedAIS);
                    AkibanInformationSchema ais = new AkibanInformationSchema();
                    AISProtobuf.AkibanInformationSchema.Builder pbAIS = AISProtobuf.AkibanInformationSchema.newBuilder();
                    new ProtobufReader(ais, pbAIS).loadBuffer(buffer);
                    return "raw:\n" + def + "\nlatest:\n" + pbAIS.build().toString();
                }
            } catch (Exception e) {
                LOG.debug("Failure to decode byPBAIS key {}", e);
            }
        }
        return defaultFilter.toValueDisplayString(exchange);
    }
}
