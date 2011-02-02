package com.akiban.cserver.service.memcache.hprocessor;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.DDLFunctions;
import com.akiban.cserver.api.DDLFunctionsImpl;
import com.akiban.cserver.api.DMLFunctions;
import com.akiban.cserver.api.DMLFunctionsImpl;
import com.akiban.cserver.api.HapiGetRequest;
import com.akiban.cserver.api.HapiProcessor;
import com.akiban.cserver.api.HapiRequestException;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.dml.scan.ColumnSet;
import com.akiban.cserver.api.dml.scan.LegacyScanRequest;
import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.api.dml.scan.NiceRow;
import com.akiban.cserver.api.dml.scan.RowDataOutput;
import com.akiban.cserver.api.dml.scan.ScanFlag;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.service.jmx.JmxManageable;
import com.akiban.cserver.service.session.Session;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class Scanrows implements HapiProcessor, JmxManageable {
    private static final String MODULE = Scanrows.class.toString();
    private static final String SESSION_BUFFER = "SESSION_BUFFER";

    private static final Scanrows instance = new Scanrows();
    private final AtomicInteger bufferSize = new AtomicInteger(65535);

    private final ScanrowsMXBean bean = new ScanrowsMXBean() {
        @Override
        public int getBufferCapacity() {
            return bufferSize.get();
        }

        @Override
        public void setBufferCapacity(int bytes) {
            bufferSize.set(bytes);
        }
    };

    public static Scanrows instance() {
        return instance;
    }

    private final DDLFunctions ddlFunctions = new DDLFunctionsImpl();
    private final DMLFunctions dmlFunctions = new DMLFunctionsImpl(ddlFunctions);

    private static class RowDataStruct {
        final NewRow start;
        final NewRow end;
        final EnumSet<ScanFlag> scanFlags;

        RowDataStruct(int tableId, RowDef rowDef) {
            start = new NiceRow(tableId, rowDef);
            end = new NiceRow(tableId, rowDef);
            scanFlags = EnumSet.noneOf(ScanFlag.class);
        }

        int scanFlagsInt() {
            return ScanFlag.toRowDataFormat(scanFlags);
        }

        RowData start() {
            return start.toRowData();
        }

        RowData end() {
            return end.toRowData();
        }
    }

    @Override
    public void processRequest(Session session, HapiGetRequest request,
                               Outputter outputter, OutputStream outputStream) throws HapiRequestException
    {
        //    public LegacyScanRequest(int tableId, RowData start, RowData end, byte[] columnBitMap, int indexId, int scanFlags)
        try {
            final int tableId;
            final int indexId;
            final byte[] columnBitMap;

            TableName tableName = new TableName(request.getSchema(), request.getTable());
            Table table = ddlFunctions.getTable(session, tableName);
            tableId = table.getTableId();
            indexId = findIndexId(table, request.getPredicates());
            columnBitMap = scanAllColumns(table);
            RowDataStruct range = getScanRange(table, request);

            LegacyScanRequest scanRequest = new LegacyScanRequest(
                    tableId, range.start(), range.end(), columnBitMap, indexId, range.scanFlagsInt());
            List<RowData> rows = RowDataOutput.scanFull(session, dmlFunctions, getBuffer(session), scanRequest);

            outputter.output(
                    request,
                    ServiceManagerImpl.get().getStore().getRowDefCache(),
                    rows,
                    outputStream
            );

        } catch (InvalidOperationException e) {
            throw new HapiRequestException("unknown error", e); // TODO
        } catch (IOException e) {
            throw new HapiRequestException("while writing output", e, HapiRequestException.ReasonCode.WRITE_ERROR);
        }
    }

    private ByteBuffer getBuffer(Session session) {
        ByteBuffer buffer = session.get(MODULE, SESSION_BUFFER);
        if (buffer == null) {
            buffer = ByteBuffer.allocate(bufferSize.get());
            session.put(MODULE, SESSION_BUFFER, buffer);
        }
        else {
            buffer.clear();
        }
        return buffer;
    }

    private RowDataStruct getScanRange(Table table, HapiGetRequest request)
            throws HapiRequestException, NoSuchTableException
    {
        if (request.getPredicates().size() != 1) {
            throw new UnsupportedOperationException();
        }
        HapiGetRequest.Predicate predicate = request.getPredicates().get(0);
        if (!table.getName().equals(predicate.getTableName())) {
            throw new UnsupportedOperationException();
        }
        if (!predicate.getOp().equals(HapiGetRequest.Predicate.Operator.EQ)) {
            throw new UnsupportedOperationException();
        }

        RowDataStruct ret = new RowDataStruct(table.getTableId(), ddlFunctions.getRowDef(table.getTableId()));

        ret.start.put( column(table, predicate), predicate.getValue() );
        ret.end.put( column(table, predicate), predicate.getValue() );
        ret.scanFlags.add(ScanFlag.START_RANGE_EXCLUSIVE);
        ret.scanFlags.add(ScanFlag.END_RANGE_EXCLUSIVE);
        ret.scanFlags.add(ScanFlag.DEEP);

        return ret;
    }

    private static int column(Table table, HapiGetRequest.Predicate predicate) {
        return table.getColumn(predicate.getColumnName()).getPosition();
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

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("HapiP-Scanrows", bean, ScanrowsMXBean.class);
    }
}
