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

import com.akiban.server.types3.TInstance;
import com.akiban.sql.optimizer.NestedResultSetTypeComputer;
import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.optimizer.plan.PhysicalSelect;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.types.AkType;
import com.akiban.server.service.functions.FunctionsRegistry;

import java.util.*;

public class PostgresJsonCompiler extends PostgresOperatorCompiler
{
    protected PostgresJsonCompiler() {
    }

    @Override
    protected void initAIS(AkibanInformationSchema ais, String defaultSchemaName) {
        super.initAIS(ais, defaultSchemaName);
        binder.setAllowSubqueryMultipleColumns(true);
    }

    @Override
    protected void initFunctionsRegistry(FunctionsRegistry functionsRegistry) {
        super.initFunctionsRegistry(functionsRegistry);
        typeComputer = new NestedResultSetTypeComputer(functionsRegistry);
    }

    public static PostgresJsonCompiler create(PostgresServerSession server) {
        PostgresJsonCompiler compiler = new PostgresJsonCompiler();
        compiler.initServer(server);
        compiler.initDone();
        return compiler;
    }

    public static class JsonResultColumn extends PhysicalResultColumn {
        private TInstance tInstance;
        private AkType akType;
        private List<JsonResultColumn> nestedResultColumns;
        
        public JsonResultColumn(String name, AkType akType, TInstance tInstance,
                                List<JsonResultColumn> nestedResultColumns) {
            super(name);
            this.akType = akType;
            this.tInstance = tInstance;
            this.nestedResultColumns = nestedResultColumns;
        }

        public AkType getAkType() {
            return akType;
        }

        public TInstance getTInstance() {
            return tInstance;
        }

        public List<JsonResultColumn> getNestedResultColumns() {
            return nestedResultColumns;
        }
    }

    @Override
    public PhysicalResultColumn getResultColumn(ResultField field) {
        return getJsonResultColumn(field.getName(), field.getSQLtype(), field.getTInstance());
    }

    protected JsonResultColumn getJsonResultColumn(String name, 
                                                   DataTypeDescriptor sqlType, TInstance tInstance) {
        AkType akType;
        List<JsonResultColumn> nestedResultColumns = null;
        if (sqlType == null)
            akType = AkType.VARCHAR;
        else if (sqlType.getTypeId().isRowMultiSet()) {
            TypeId.RowMultiSetTypeId typeId = 
                (TypeId.RowMultiSetTypeId)sqlType.getTypeId();
            String[] columnNames = typeId.getColumnNames();
            DataTypeDescriptor[] columnTypes = typeId.getColumnTypes();
            nestedResultColumns = new ArrayList<JsonResultColumn>(columnNames.length);
            for (int i = 0; i < columnNames.length; i++) {
                nestedResultColumns.add(getJsonResultColumn(columnNames[i], columnTypes[i],
                        TypesTranslation.toTInstance(columnTypes[i])));
            }
            akType = AkType.RESULT_SET;
        }
        else
            akType = TypesTranslation.sqlTypeToAkType(sqlType);
        return new JsonResultColumn(name, akType, tInstance, nestedResultColumns);
    }

    @Override
    protected PostgresStatement generateSelect(PhysicalSelect select,
                                               PostgresType[] parameterTypes) {
        int ncols = select.getResultColumns().size();
        List<JsonResultColumn> resultColumns = new ArrayList<JsonResultColumn>(ncols);
        for (PhysicalResultColumn physColumn : select.getResultColumns()) {
            JsonResultColumn resultColumn = (JsonResultColumn)physColumn;
            resultColumns.add(resultColumn);
        }
        return new PostgresJsonStatement(select.getResultOperator(),
                                         select.getResultRowType(),
                                         resultColumns,
                                         parameterTypes,
                                         usesPValues());
    }
    
}
