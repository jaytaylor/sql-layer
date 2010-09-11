package com.akiban.cserver.store;

import com.persistit.Exchange;
import com.persistit.Transaction;
import com.persistit.TransactionRunnable;
import com.persistit.Value;
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.ValueCoder;
import com.persistit.exception.ConversionException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

public final class PersistitStorePropertiesManager {
    private final static String PROPERTIES_TREE_NAME = "_properties_";

    private final static String SCHEMA_ID = "schema.id";

    private final PersistitStore store;

    private final static ValueCoder SCHEMA_ID_VALUE_CODER = new ValueCoder() {
        @Override
        public void put(Value value, Object object, CoderContext context) throws ConversionException {
            SchemaId schemaId = (SchemaId) object;
            value.put(schemaId.getGeneration());
        }

        @Override
        public Object get(Value value, Class clazz, CoderContext context) throws ConversionException {
            assert SchemaId.class.equals(clazz) : clazz;
            int generation = value.getInt();
            return new SchemaId(generation);
        }
    };

    public PersistitStorePropertiesManager(PersistitStore store) {
        this.store = store;
    }

    public void startUp() {
        store.getDb().getCoderManager().registerValueCoder(SchemaId.class, SCHEMA_ID_VALUE_CODER);
    }

    private static SchemaId schemaIdFromExchange(Exchange ex) throws PersistitException {
        ex.clear().append(SCHEMA_ID).fetch();
        return ex.getValue().isDefined()
               ? (SchemaId) ex.getValue().get()
               : new SchemaId(0);
    }

    public SchemaId getSchemaId() throws PersistitException {
        Exchange ex = store.getExchange(PROPERTIES_TREE_NAME);
        try {
            return schemaIdFromExchange(ex);
        } finally {
            store.releaseExchange(ex);
        }
    }

    public void incrementSchemaId() throws PersistitException {
        Transaction transaction = store.getDb().getTransaction();
        transaction.run(new TransactionRunnable(){
            @Override
            public void runTransaction() throws PersistitException, RollbackException {
                Exchange ex = store.getExchange(PROPERTIES_TREE_NAME);
                SchemaId schemaId = schemaIdFromExchange(ex);
                schemaId.incrementGeneration();
                ex.getValue().put(schemaId);
                ex.store();
            }
        });
    }
}
