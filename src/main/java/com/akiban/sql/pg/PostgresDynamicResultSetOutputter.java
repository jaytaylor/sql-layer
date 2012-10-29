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

package com.akiban.sql.pg;

import com.akiban.server.error.ExternalRoutineInvocationException;
import com.akiban.server.error.SQLParserInternalException;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.sql.StandardException;
import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PostgresDynamicResultSetOutputter extends PostgresOutputter<ResultSet>
{
    private int ncols;
    private PostgresType[] columnTypes;
    private String[] columnNames;

    public PostgresDynamicResultSetOutputter(PostgresQueryContext context,
                                             PostgresJavaRoutine statement) {
        super(context, statement);
    }

    public void setMetaData(ResultSetMetaData metaData) throws SQLException {
        ncols = metaData.getColumnCount();
        columnTypes = new PostgresType[ncols];
        columnNames = new String[ncols];
        for (int i = 0; i < ncols; i++) {
            columnTypes[i] = typeFromSQL(metaData, i+1);
            columnNames[i] = metaData.getColumnName(i+1);
        }
    }

    public void sendDescription() throws IOException {
        messenger.beginMessage(PostgresMessages.ROW_DESCRIPTION_TYPE.code());
        messenger.writeShort(ncols);
        for (int i = 0; i < columnTypes.length; i++) {
            PostgresType type = columnTypes[i];
            messenger.writeString(columnNames[i]); // attname
            messenger.writeInt(0);    // attrelid
            messenger.writeShort(0);  // attnum
            messenger.writeInt(type.getOid()); // atttypid
            messenger.writeShort(type.getLength()); // attlen
            messenger.writeInt(type.getModifier()); // atttypmod
            messenger.writeShort(0);
        }
        messenger.sendMessage();
    }

    @Override
    public void output(ResultSet resultSet, boolean usePVals) throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(ncols);
        for (int i = 0; i < ncols; i++) {
            Object column;
            try {
                column = resultSet.getObject(i+1);
            }
            catch (SQLException ex) {
                throw new ExternalRoutineInvocationException(((PostgresJavaRoutine)statement).getInvocation().getRoutineName(), ex);
            }
            PostgresType type = columnTypes[i];
            boolean binary = false;
            ByteArrayOutputStream bytes;
            if (usePVals) bytes = encoder.encodePObject(column, type, binary);
            else bytes = encoder.encodeObject(column, type, binary);
            if (bytes == null) {
                messenger.writeInt(-1);
            }
            else {
                messenger.writeInt(bytes.size());
                messenger.writeByteStream(bytes);
            }
        }
        messenger.sendMessage();
    }

    protected static PostgresType typeFromSQL(ResultSetMetaData metaData, int columnIndex) throws SQLException {
        TypeId typeId = TypeId.getBuiltInTypeId(metaData.getColumnType(columnIndex));
        if (typeId == null) {
            try {
                typeId = TypeId.getUserDefinedTypeId(metaData.getColumnTypeName(columnIndex),
                                                     false);
            }
            catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
        }
        DataTypeDescriptor sqlType;
        if (typeId.isDecimalTypeId() || typeId.isNumericTypeId()) {
            sqlType = new DataTypeDescriptor(typeId,
                                             metaData.getPrecision(columnIndex),
                                             metaData.getScale(columnIndex),
                                             metaData.isNullable(columnIndex) != ResultSetMetaData.columnNoNulls,
                                             metaData.getColumnDisplaySize(columnIndex));
        }
        else {
            sqlType = new DataTypeDescriptor(typeId,
                                             metaData.isNullable(columnIndex) != ResultSetMetaData.columnNoNulls,
                                             metaData.getColumnDisplaySize(columnIndex));
        }
        AkType akType = TypesTranslation.sqlTypeToAkType(sqlType);
        TInstance tInstance = TypesTranslation.toTInstance(sqlType);
        return PostgresType.fromDerby(sqlType, akType, tInstance);
    }

}
