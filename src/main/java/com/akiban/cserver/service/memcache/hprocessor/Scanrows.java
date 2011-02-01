package com.akiban.cserver.service.memcache.hprocessor;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.DDLFunctions;
import com.akiban.cserver.api.DDLFunctionsImpl;
import com.akiban.cserver.api.DMLFunctions;
import com.akiban.cserver.api.DMLFunctionsImpl;
import com.akiban.cserver.api.HapiGetRequest;
import com.akiban.cserver.api.HapiProcessor;
import com.akiban.cserver.api.HapiRequestException;
import com.akiban.cserver.api.dml.scan.ColumnSet;
import com.akiban.cserver.service.session.Session;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Scanrows implements HapiProcessor {
    private static final Scanrows instance = new Scanrows();

    public static Scanrows instance() {
        return instance;
    }

    private final DDLFunctions ddlFunctions;
    private final DMLFunctions dmlFunctions;

    private Scanrows() {
        DDLFunctionsImpl ddl = new DDLFunctionsImpl();
        ddlFunctions = ddl;
        dmlFunctions = new DMLFunctionsImpl(ddl);
    }

    @Override
    public void processRequest(Session session, HapiGetRequest request,
                               Outputter outputter, OutputStream outputStream) throws HapiRequestException
    {
        //    public LegacyScanRequest(int tableId, RowData start, RowData end, byte[] columnBitMap, int indexId, int scanFlags)
        try {
            final int tableId;
            final int indexId;
            final int scanFlags;
            final byte[] columnBitMap;

            TableName tableName = new TableName(request.getSchema(), request.getTable());
            Table table = ddlFunctions.getTable(session, tableName);
            tableId = table.getTableId();
            indexId = findIndexId(table, request.getPredicates());
            columnBitMap = scanAllColumns(table);

        } catch (InvalidOperationException e) {
            throw new HapiRequestException("unknown error", e); // TODO
        }
    }

    private static byte[] scanAllColumns(Table table) {
        Set<Integer> allSet = new HashSet<Integer>();
        for (int i=0; i < table.getColumns().size(); ++i) {
            allSet.add(i);
        }
        return ColumnSet.packToLegacy(allSet);
    }

    /**
     * Find an index that contains all of the columns specified in the predicates, and no more.
     * If the primary key matches, it is always returned. Otherwise, if more than one index matches, an
     * exception is thrown.
     * @param table the table to look in
     * @param predicates the predicates; each one's table name must match the given table
     * @return the index id
     * @throws HapiRequestException if the index can't be deduced
     */
    private static int findIndexId(Table table, List<HapiGetRequest.Predicate> predicates) throws HapiRequestException {
        List<String> columns = new ArrayList<String>();
        for (HapiGetRequest.Predicate predicate : predicates) {
            if (!table.getName().equals(predicate.getTableName())) {
                throw new HapiRequestException(
                        String.format("predicate %s must be against table %s", predicate, table.getName()),
                        HapiRequestException.ReasonCode.UNSUPPORTED_REQUEST
                );
            }
            columns.add(predicate.getColumnName());
        }

        if (table instanceof UserTable) {
            UserTable utable = (UserTable) table;
            PrimaryKey pk = utable.getPrimaryKey();
            if (pk != null) {
                Index pkIndex = pk.getIndex();
                if (allColumnsInIndex(pkIndex, columns)) {
                    return pkIndex.getIndexId();
                }
            }
        }
        List<Index> candidateIndexes = new ArrayList<Index>(table.getIndexes());

        Index bestMatch = null;
        for (Index index : candidateIndexes) {
            if (index.isPrimaryKey()) {
                continue; // already saw it
            }

            if (allColumnsInIndex(index, columns)) {
                if (bestMatch != null) {
                    throw new HapiRequestException(
                            String.format("Can't guess index; between %s and %s",
                                    bestMatch.getIndexName(), index.getIndexName()
                            ),
                            HapiRequestException.ReasonCode.UNSUPPORTED_REQUEST
                    );
                }
                bestMatch = index;
            }
        }
        if (bestMatch == null) {
            throw new HapiRequestException("no appropriate index found",
                    HapiRequestException.ReasonCode.UNSUPPORTED_REQUEST
            );
        }
        return bestMatch.getIndexId();
    }

    private static boolean allColumnsInIndex(Index index, List<String> columns) {
        List<String> indexColumns = new ArrayList<String>(index.getColumns().size());
        for (IndexColumn indexColumn : index.getColumns()) {
            indexColumns.add(indexColumn.getColumn().getName());
        }
        return indexColumns.equals(columns);
    }
}
