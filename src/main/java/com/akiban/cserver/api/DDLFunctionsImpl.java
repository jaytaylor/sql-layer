package com.akiban.cserver.api;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.ddl.*;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.store.SchemaId;
import com.akiban.cserver.store.Store;
import com.akiban.message.ErrorCode;
import com.akiban.ais.io.*;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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

    @Override
    public void createIndexes(AkibaInformationSchema ais) throws InvalidOperationException {
        try {
            if(ais.getUserTables().size() != 1) {
                throw new Exception("Too many user tables");
            }
            
            Entry<TableName, UserTable> index_entry = ais.getUserTables().entrySet().iterator().next();
            AkibaInformationSchema cur_ais = getAIS();
            UserTable cur_utable = cur_ais.getUserTable(index_entry.getKey());
            
            if(cur_utable == null) {
                throw new Exception("Uknown table");
            }
            
            SortedSet<Integer> cur_ids = new TreeSet<Integer>();
            Set<IndexName> cur_names = new HashSet<IndexName>();
            for(Index i : cur_utable.getIndexes()) {
                cur_ids.add(i.getIndexId());
                cur_names.add(i.getIndexName());
            }
            
            Integer id_start = cur_ids.last() + 1;
            for(Index i: index_entry.getValue().getIndexes()) {
                if(cur_names.contains(i.getIndexName())) {
                    throw new Exception("Duplicate index name");
                }
                
                i.setIndexId(id_start);
                ++id_start;
                
                System.out.println("New index ok: " + i.getIndexName().getName() + ", id: " + i.getIndexId());
            }
            
            // All were valid, add to current AIS
            for(Index i: index_entry.getValue().getIndexes()) {
                Index new_idx = Index.create(cur_ais, cur_utable, i.getIndexName().getName(), i.getIndexId(), i.isUnique(), i.getConstraint());
                
                for(IndexColumn c : i.getColumns()) {
                    Column ref_col = cur_utable.getColumn(c.getColumn().getPosition());
                    IndexColumn icol = new IndexColumn(new_idx, ref_col, c.getPosition(), c.isAscending(), c.getIndexedLength()); 
                    new_idx.addColumn(icol);
                }
            }
            
            // Modify stored DDL statement
            DDLGenerator gen = new DDLGenerator();
            schemaManager().changeTableDDL(cur_utable.getName().getSchemaName(), 
                                           cur_utable.getName().getTableName(),
                                           gen.createTable(cur_utable));
        } 
        catch (Exception e) {
            throw new InvalidOperationException(ErrorCode.UNEXPECTED_EXCEPTION, "Unexpected exception", e);
        }
    }

    @Override
    public void dropIndexes(TableId tableId, List<Integer> indexIds) throws InvalidOperationException {
        // TODO Auto-generated method stub
    }
}
