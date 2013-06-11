/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.akiban.server.service.restdml;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.Store;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;

public class DeleteProcessor extends DMLProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteProcessor.class);
    private OperatorGenerator deleteGenerator;
    
    public DeleteProcessor (
            Store store,
            T3RegistryService t3RegistryService) {
        super (store, t3RegistryService);
    }

    private static final CacheValueGenerator<DeleteGenerator> CACHED_DELETE_GENERATOR =
            new CacheValueGenerator<DeleteGenerator>() {
                @Override
                public DeleteGenerator valueFor(AkibanInformationSchema ais) {
                    return new DeleteGenerator(ais);
                }
            };

    public void processDelete (Session session, AkibanInformationSchema ais, TableName tableName, String identifiers) {
        ProcessContext context = new ProcessContext (ais, session, tableName);
        
        deleteGenerator = getGenerator (CACHED_DELETE_GENERATOR, context);

        Index pkIndex = context.table.getPrimaryKeyIncludingInternal().getIndex();
        List<List<String>> pks = PrimaryKeyParser.parsePrimaryKeys(identifiers, pkIndex);
        
        PValue pvalue = new PValue(MString.VARCHAR.instance(Integer.MAX_VALUE, false));
        Cursor cursor = null;

        try {
            Operator delete = deleteGenerator.get(tableName);
            cursor = API.cursor(delete, context.queryContext);

            for (List<String> key : pks) {
                for (int i = 0; i < key.size(); i++) {
                    String akey = key.get(i);
                    pvalue.putString(akey, null);
                    context.queryContext.setPValue(i, pvalue);
                }
    
                cursor.open();
                Row row;
                while ((row = cursor.next()) != null) {
                    // Do Nothing - the act of reading the cursor 
                    // does the delete row processing.
                    // TODO: Check that we got 1 row through.
                }
                cursor.close();
            }
        } finally {
            if (cursor != null)
                cursor.destroy();

        }
    }
}
