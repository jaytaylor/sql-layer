package com.akiban.server.service.restdml;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;

public class DeleteProcessor extends DMLProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteProcessor.class);
    private OperatorGenerator deleteGenerator;
    
    public DeleteProcessor (ConfigurationService configService, 
            TreeService treeService, 
            Store store,
            T3RegistryService t3RegistryService) {
        super (configService, treeService, store, t3RegistryService);
    }

    private static final CacheValueGenerator<DeleteGenerator> CACHED_DELETE_GENERATOR =
            new CacheValueGenerator<DeleteGenerator>() {
                @Override
                public DeleteGenerator valueFor(AkibanInformationSchema ais) {
                    return new DeleteGenerator(ais);
                }
            };

    public void processDelete (Session session, AkibanInformationSchema ais, TableName tableName, String identifiers) {
        setAIS (ais);
        deleteGenerator = getGenerator (CACHED_DELETE_GENERATOR);

        UserTable table = getTable (tableName);
        QueryContext queryContext = newQueryContext (session, table);

        Index pkIndex = table.getPrimaryKeyIncludingInternal().getIndex();
        List<List<String>> pks = PrimaryKeyParser.parsePrimaryKeys(identifiers, pkIndex);
        
        PValue pvalue = new PValue(MString.VARCHAR.instance(Integer.MAX_VALUE, false));
        Cursor cursor = null;

        try {
            Operator delete = deleteGenerator.get(tableName);
            cursor = API.cursor(delete, queryContext);

            for (List<String> key : pks) {
                for (int i = 0; i < key.size(); i++) {
                    String akey = key.get(i);
                    pvalue.putString(akey, null);
                    queryContext.setPValue(i, pvalue);
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
