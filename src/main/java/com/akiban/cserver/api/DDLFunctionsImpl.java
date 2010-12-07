package com.akiban.cserver.api;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.ddl.*;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.store.SchemaId;
import com.akiban.cserver.store.Store;
import com.akiban.message.ErrorCode;

import java.util.List;

public final class DDLFunctionsImpl extends ClientAPIBase implements DDLFunctions {
    
    public static DDLFunctions instance() {
        return new DDLFunctionsImpl(getDefaultStore());
    }

    public DDLFunctionsImpl(Store store) {
        super(store);
    }

    @Override
    public void createTable(String schema, String ddlText)
    throws ParseException,
            UnsupportedCharsetException,
            ProtectedTableDDLException,
            DuplicateTableNameException,
            GroupWithProtectedTableException,
            JoinToUnknownTableException,
            JoinToWrongColumnsException,
            NoPrimaryKeyException,
            DuplicateColumnNameException,
            GenericInvalidOperationException
    {
        try {
            schemaManager().createTable(schema, ddlText);
        } catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    @Override
    public void dropTable(TableId tableId)
    throws  ProtectedTableDDLException,
            ForeignConstraintDDLException,
            GenericInvalidOperationException
    {
        final TableName tableName;
        try {
            tableName = tableId.getTableName(idResolver());
        }
        catch (NoSuchTableException e) {
            return; // dropping a nonexistent table is a no-op
        }
        
        try {
            schemaManager().dropTable(tableName.getSchemaName(), tableName.getTableName());
        }
        catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    @Override
    public void dropSchema(String schemaName)
            throws  ProtectedTableDDLException,
            ForeignConstraintDDLException,
            GenericInvalidOperationException
    {
        try {
            schemaManager().dropSchema(schemaName);
        }
        catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    @Override
    public AkibaInformationSchema getAIS() {
        return store().getAis();
    }

    @Override
    public TableName getTableName(TableId tableId) throws NoSuchTableException {
        return tableId.getTableName(idResolver());
    }

    @Override
    public TableId resolveTableId(TableId tableId) throws NoSuchTableException {
        tableId.getTableId(idResolver());
        tableId.getTableName(idResolver());
        return tableId;
    }

    @Override
    public List<String> getDDLs() throws InvalidOperationException {
        try {
            return schemaManager().getDDLs();
        } catch (Exception e) {
            throw new InvalidOperationException(ErrorCode.UNEXPECTED_EXCEPTION, "Unexpected exception", e);
        }
    }

    @Override
    public SchemaId getSchemaID() throws InvalidOperationException {
        try {
            return schemaManager().getSchemaID();
        } catch (Exception e) {
            throw new InvalidOperationException(ErrorCode.UNEXPECTED_EXCEPTION, "Unexpected exception", e);
        }
    }

    @Override
    @SuppressWarnings("unused") // meant to be used from JMX
    public void forceGenerationUpdate() throws InvalidOperationException {
        try {
            schemaManager().forceSchemaGenerationUpdate();
        } catch (Exception e) {
            throw new InvalidOperationException(ErrorCode.UNEXPECTED_EXCEPTION, "Unexpected exception", e);
        }
    }
}
