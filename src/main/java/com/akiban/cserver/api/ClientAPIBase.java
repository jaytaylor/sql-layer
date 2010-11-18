package com.akiban.cserver.api;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.IdResolverImpl;
import com.akiban.cserver.manage.SchemaManager;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.store.Store;
import com.akiban.util.ArgumentValidation;

abstract class ClientAPIBase {

    private final Store store;
    private final IdResolverImpl resolver;

    /**
     * Use this only for debugging!
     */
    ClientAPIBase(String confirmation) {
        if (!"DEBUG".equalsIgnoreCase(confirmation)) {
            throw new RuntimeException();
        }
        store = null;
        resolver = null;
    }


    ClientAPIBase(Store store) {
        ArgumentValidation.notNull("schema manager", store);
        this.store = store;
        this.resolver = new IdResolverImpl(store.getSchemaManager());
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
     * @throws com.akiban.cserver.api.ddl.DDLException the specific exception
     * @return nothing; this method will always throw an InvalidOperationException
     */
    static InvalidOperationException rethrow(Exception e) throws InvalidOperationException {
        if (! (e instanceof InvalidOperationException)) {
            throw new InvalidOperationException(e);
        }
        final InvalidOperationException ioe = (InvalidOperationException)e;
        switch (ioe.getCode()) {
            // TODO FINISH THIS
//            case PARSE_EXCEPTION:
//                throw new ParseException(ioe);
            default:
                throw ioe;
        }
    }
}
