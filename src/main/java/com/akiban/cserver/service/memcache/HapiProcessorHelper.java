package com.akiban.cserver.service.memcache;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiProcessor;
import com.akiban.cserver.api.HapiRequestException;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.store.Store;
import com.akiban.util.ArgumentValidation;

final class HapiProcessorHelper {

	static void processRequest(Store store, Session session, String request,
                                 ByteBuffer byteBuffer, HapiProcessor.Outputter outputter, OutputStream outputStream)
            throws HapiRequestException, IOException
    {
        ArgumentValidation.notNull("outputter", outputter);
        String[] tokens = request.split(":");

        if(tokens.length == 3 || tokens.length == 4) {
            String schema = tokens[0];
            String table = tokens[1];
            String colkey = tokens[2];
            String min_val = null;
            String max_val = null;


            if(tokens.length == 4) {
                min_val = max_val = tokens[3];
            }

            final RowDefCache cache = store.getRowDefCache();
            final List<RowData> list;
            try {
                list = store.fetchRows(session, schema, table, colkey, min_val, max_val, null, byteBuffer);
            } catch (Exception e) {
                throw new HapiRequestException("while fetching rows", e);
            }
            outputter.output(cache, list, outputStream);

        }
        else {
            throw new HapiRequestException("not enough tokens: " + request, HapiRequestException.ReasonCode.UNPARSABLE);
        }
	}
}
