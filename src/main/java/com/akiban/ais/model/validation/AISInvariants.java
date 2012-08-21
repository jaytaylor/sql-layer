/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.server.error.AISNullReferenceException;
import com.akiban.server.error.DuplicateColumnNameException;
import com.akiban.server.error.DuplicateGroupNameException;
import com.akiban.server.error.DuplicateIndexColumnException;
import com.akiban.server.error.DuplicateIndexException;
import com.akiban.server.error.DuplicateSequenceNameException;
import com.akiban.server.error.DuplicateTableNameException;
import com.akiban.server.error.NameIsNullException;

public class AISInvariants {

    public static void checkNullField (Object field, String owner, String fieldName, String reference) {
        if (field == null) {
            throw new AISNullReferenceException(owner, fieldName, reference);
        }
    }
    
    public static void checkNullName (final String name, final String source, final String type) {
        if (name == null || name.length() == 0) {
            throw new NameIsNullException (source, type);
        }
    }
    
    public static void checkDuplicateTables(AkibanInformationSchema ais, String schemaName, String tableName)
    {
        if (ais.getColumnar(schemaName, tableName) != null) {
            throw new DuplicateTableNameException (new TableName(schemaName, tableName));
        }
    }
    
    public static void checkDuplicateSequence(AkibanInformationSchema ais, String schemaName, String sequenceName)
    {
        if (ais.getSequence(new TableName (schemaName, sequenceName)) != null) {
            throw new DuplicateSequenceNameException (new TableName(schemaName, sequenceName));
        }
    }
    
    public static void checkDuplicateColumnsInTable(Columnar table, String columnName)
    {
        if (table.getColumn(columnName) != null) {
            throw new DuplicateColumnNameException(table.getName(), columnName);
        }
    }
    public static void checkDuplicateColumnPositions(Columnar table, Integer position) {
        if (position < table.getColumnsIncludingInternal().size() && 
                table.getColumn(position) != null &&
                table.getColumn(position).getPosition().equals(position)) {
            throw new DuplicateColumnNameException (table.getName(), table.getColumn(position).getName());
        }
    }
    
    public static void checkDuplicateColumnsInIndex(Index index, TableName columnarName, String columnName)
    {
        for(IndexColumn icol : index.getKeyColumns()) {
            Column column = icol.getColumn();
            if(column.getName().equals(columnName) && column.getColumnar().getName().equals(columnarName)) {
                throw new DuplicateIndexColumnException (index, columnName);
            }
        }
    }
    
    public static void checkDuplicateIndexesInTable(Table table, String indexName) 
    {
        if (isIndexInTable(table, indexName)) {
            throw new DuplicateIndexException (table.getName(), indexName);
        }
    }
    
    public static boolean isIndexInTable (Table table, String indexName)
    {
        return table.getIndex(indexName) != null;
    }
 
    public static void checkDuplicateIndexColumnPosition (Index index, Integer position) {
        if (position < index.getKeyColumns().size()) {
            // TODO: Index uses position for a relative ordering, not an absolute position. 
        }
    }
    public static void checkDuplicateGroups (AkibanInformationSchema ais, String groupName)
    {
        if (ais.getGroup(groupName) != null) {
            throw new DuplicateGroupNameException (groupName);
        }
    }    
}
