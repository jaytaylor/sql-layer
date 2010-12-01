package com.akiban.cserver.api;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.common.IdResolverImpl;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.NoSuchTableException;
import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.api.dml.scan.NiceRow;
import com.akiban.cserver.manage.SchemaManager;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.store.Store;
import com.akiban.util.ArgumentValidation;

abstract class ClientAPIBase implements LegacyConverter {

    private final Store store;
    private final IdResolverImpl resolver;

    ClientAPIBase(Store store) {
        ArgumentValidation.notNull("schema manager", store);
        this.store = store;
        this.resolver = new IdResolverImpl(store);
    }

    final public Store store() {
        return store;
    }

    final public SchemaManager schemaManager() {
        return store.getSchemaManager();
    }

    final public IdResolverImpl idResolver() {
        return resolver;
    }

    @Override
    public NewRow convertRowData(RowData rowData) throws NoSuchTableException {
        RowDef rowDef = idResolver().getRowDef( TableId.of(rowData.getRowDefId()) );
        return NiceRow.fromRowData(rowData, rowDef);
    }

    @Override
    public NewRow[] convertRowDatas(RowData... rowDatas) throws NoSuchTableException {
        if (rowDatas.length == 0) {
            return new NewRow[0];
        }

        NewRow[] retval = new NewRow[rowDatas.length];
        int lastRowDefId = -1; rowDatas[0].getRowDefId();
        RowDef rowDef = null;
        for (int i=0; i < retval.length; ++i) {
            int currRowDefId = rowDatas[i].getRowDefId();
            if ( (rowDef == null) || (currRowDefId != lastRowDefId) ) {
                lastRowDefId = currRowDefId;
                rowDef = idResolver().getRowDef( TableId.of(currRowDefId) );
            }
            retval[i] = NiceRow.fromRowData(rowDatas[i], rowDef);
        }
        return retval;
    }

    static Store getDefaultStore() {
        ServiceManager serviceManager = ServiceManagerImpl.get();
        if (serviceManager == null) {
            throw new RuntimeException("ServiceManager was not installed");
        }
        Store store = serviceManager.getStore();
        if (store == null) {
            throw new RuntimeException("ServiceManager had no Store");
        }
        return store;
    }

    /**
     * Throws a specific DDLException based on the invalid operation exception specified. This method will always
     * throw an exception, but it is listed as having a return value so that you can tell the compiler you're
     * throwing the result; this helps the compiler compute execution paths.
     * @param e the cause
     * @throws GenericInvalidOperationException if the given exception isn't recognized
     * @return nothing; this method will always throw an InvalidOperationException
     */
    static GenericInvalidOperationException rethrow(Exception e) throws GenericInvalidOperationException {
        if (! (e instanceof InvalidOperationException)) {
            throw new GenericInvalidOperationException(e);
        }
        final InvalidOperationException ioe = (InvalidOperationException)e;
        switch (ioe.getCode()) {
            // TODO FINISH THIS
//            case PARSE_EXCEPTION:
//                throw new ParseException(ioe);
            default:
                throw new GenericInvalidOperationException(ioe);
        }
    }
}
