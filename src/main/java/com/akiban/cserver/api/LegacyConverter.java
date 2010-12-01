package com.akiban.cserver.api;

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.dml.NoSuchTableException;
import com.akiban.cserver.api.dml.scan.NewRow;

/**
 * Provides simple conversion from RowData to NiceRow. This hides the step of looking up the RowDef, which
 * callers may not have access to.
 */
public interface LegacyConverter {
    /**
     * Converts a RowData to a NiceRow
     * @param rowData the RowData to convert
     * @return the converted RowData
     * @throws NoSuchTableException if the tableID specified by the RowData can't be resolved.
     * @throws NullPointerException if rowData is null
     */
    NewRow convertRowData(RowData rowData) throws NoSuchTableException;

    /**
     * Converts an array of RowDatas to an array of NiceRows. For every index <tt>i</tt> in <tt>rowDatas[]</tt>,
     * the there will be an element <tt>NiceRow[i]</tt> that was converted from the RowData. The returned array
     * will have the same length as the incoming array.
     * @param rowDatas the RowData to convert
     * @return the converted RowData
     * @throws NoSuchTableException if the tableID specified by the any RowData element can't be resolved.
     * @throws NullPointerException if rowDatas or any of its elements are null
     */
    NewRow[] convertRowDatas(RowData... rowDatas) throws NoSuchTableException;
}
