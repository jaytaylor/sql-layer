package com.akiban.cserver.api;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.IdResolverImpl;
import com.akiban.cserver.api.dml.NoSuchRowException;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.store.SchemaManager;
import com.akiban.cserver.store.Store;

abstract class ClientAPIBase {

    private final Store store;
    private final SchemaManager schemaManager;
    private final IdResolverImpl resolver;

    ClientAPIBase() {
        final ServiceManager serviceManager = ServiceManagerImpl.get();
        this.store = serviceManager.getStore();
        this.schemaManager = serviceManager.getSchemaManager();
        this.resolver = new IdResolverImpl(store);
    }

    final public Store store() {
        return store;
    }

    final public SchemaManager schemaManager() {
        return schemaManager;
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
     * Returns an exception as an InvalidOperationException. If the given
     * exception is one that we know how to turn into a specific
     * InvalidOperationException (e.g., NoSuchRowException), the returned
     * exception will be of that type. Otherwise, if the given exception is an
     * InvalidOperationException, we'll just return it, and if not, we'll wrap
     * it in a GenericInvalidOperationException.
     * 
     * @param e
     *            the exception to wrap
     * @return as specific an InvalidOperationException as we know how to make
     */
    protected static InvalidOperationException launder(Exception e) {
        if (e instanceof InvalidOperationException) {
            final InvalidOperationException ioe = (InvalidOperationException) e;
            switch (ioe.getCode()) {
            case NO_SUCH_RECORD:
                return new NoSuchRowException(ioe);
            default:
                return ioe;
            }
        }
        return new GenericInvalidOperationException(e);
    }

    /**
     * Throws the given InvalidOperationException, downcast, if it's of the
     * appropriate type
     * 
     * @param cls
     *            the class to check for and cast to
     * @param e
     *            the exception to check
     * @param <T>
     *            an InvalidOperationException to throw as
     * @throws T
     *             the e instance, cast down
     */
    protected static <T extends InvalidOperationException> void throwIfInstanceOf(
            Class<T> cls, InvalidOperationException e) throws T {
        if (cls.isInstance(e)) {
            throw cls.cast(e);
        }
    }
}
