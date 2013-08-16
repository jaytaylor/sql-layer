/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.foundationdb.server.service.restdml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.UserTableRowType;
import com.foundationdb.server.t3expressions.OverloadResolver;
import com.foundationdb.server.t3expressions.OverloadResolver.OverloadResult;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types3.TCast;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValue;
import com.foundationdb.server.types3.pvalue.PValueSources;
import com.foundationdb.server.types3.texpressions.TCastExpression;
import com.foundationdb.server.types3.texpressions.TNullExpression;
import com.foundationdb.server.types3.texpressions.TPreparedExpression;
import com.foundationdb.server.types3.texpressions.TPreparedField;
import com.foundationdb.server.types3.texpressions.TPreparedFunction;
import com.foundationdb.server.types3.texpressions.TPreparedLiteral;
import com.foundationdb.server.types3.texpressions.TValidatedScalar;

public class InsertGenerator extends OperatorGenerator{

    private UserTable table;
    
    public InsertGenerator (AkibanInformationSchema ais) {
        super(ais);
    }
    
    @Override
    protected Operator create(TableName tableName) {
        
        table = ais().getUserTable(tableName);

        RowStream stream = assembleValueScan (table);
        stream = assembleProjectTable (stream, table);
        stream.operator = API.insert_Returning(stream.operator);
        stream = projectTable(stream, table);
        return stream.operator; 
    }
    
    protected RowStream assembleProjectTable (RowStream input, UserTable table) {
        
        int nfields = input.rowType.nFields();
        List<TPreparedExpression> insertsP = null;
        UserTableRowType targetRowType = schema().userTableRowType(table);
        insertsP = new ArrayList<>(targetRowType.nFields());
        
        for (int i = 0; i < nfields; ++i) {
            insertsP.add(new TPreparedField(input.rowType.typeInstanceAt(i), i));
        }

        TPreparedExpression[] row = new TPreparedExpression[targetRowType.nFields()];
        for (int i = 0; i < nfields; i++) {
            row[i] = new TPreparedField (input.rowType.typeInstanceAt(i), i);
        }
        // Insert the sequence generator and column default values
        for (int i = 0, len = targetRowType.nFields(); i < len; ++i) {
            Column column = table.getColumnsIncludingInternal().get(i);

            if (row[i] == null) {
                TInstance tinst = targetRowType.typeInstanceAt(i);
                final PValue defaultValueSource = new PValue(tinst);
                defaultValueSource.putNull();
                row[i] = new TPreparedLiteral(tinst, defaultValueSource);
            } else if (!column.tInstance().equals(row[i].resultType())) {
                TCast cast = registryService().getCastsResolver().cast(row[i].resultType().typeClass(), 
                        column.tInstance().typeClass()); 
                row[i] = new TCastExpression(row[i], cast, column.tInstance(), queryContext());
            }
            
            if (column.getIdentityGenerator() != null) {
                Sequence sequence = table.getColumn(i).getIdentityGenerator();
                row[i] = sequenceGenerator(sequence, column, row[i]);
            } else if (column.getDefaultValue() != null) {
                final String defaultValue = column.getDefaultValue();
                final PValue defaultValueSource;
                TInstance tinst = targetRowType.typeInstanceAt(i);
                TCast cast = tinst.typeClass().castFromVarchar();
                if (cast != null) {
                    defaultValueSource = new PValue(tinst);
                    TInstance valInst = MString.VARCHAR.instance(defaultValue.length(), false);
                    TExecutionContext executionContext = new TExecutionContext(
                            Collections.singletonList(valInst),
                            tinst, queryContext());
                    cast.evaluate(executionContext,
                                  new PValue(MString.varcharFor(defaultValue), defaultValue),
                                  defaultValueSource);
                } else {
                    defaultValueSource = new PValue (tinst, defaultValue);
                }
                row[i] = generateIfNull (insertsP.get(i), new TPreparedLiteral(tinst, defaultValueSource));
            }
        }
        insertsP = Arrays.asList(row);
        
        input.operator = API.project_Table(input.operator, input.rowType,
                targetRowType, null, insertsP);
        input.rowType = targetRowType;
        return input;
    }

    public TPreparedExpression sequenceGenerator(Sequence sequence, Column column, TPreparedExpression expression) {
        OverloadResolver<TValidatedScalar> resolver = registryService().getScalarsResolver();
        TInstance instance = column.tInstance();
        
        List<TPreptimeValue> input = new ArrayList<>(2);
        input.add(PValueSources.fromObject(sequence.getSequenceName().getSchemaName(), AkType.VARCHAR));
        input.add(PValueSources.fromObject(sequence.getSequenceName().getTableName(), AkType.VARCHAR));
    
        TValidatedScalar overload = resolver.get("NEXTVAL", input).getOverload();
    
        List<TPreparedExpression> arguments = new ArrayList<>(2);
        arguments.add(new TPreparedLiteral(input.get(0).instance(), input.get(0).value()));
        arguments.add(new TPreparedLiteral(input.get(1).instance(), input.get(1).value()));
    
        TInstance overloadResultInstance = overload.resultStrategy().fixed(column.getNullable());
        TPreparedExpression seqExpr =  new TPreparedFunction(overload, overloadResultInstance,
                        arguments, queryContext());
    
        if (!instance.equals(overloadResultInstance)) {
            TCast tcast = registryService().getCastsResolver().cast(seqExpr.resultType(), instance);
            seqExpr = 
                    new TCastExpression(seqExpr, tcast, instance, queryContext());
        }
        // If the row expression is not null (i.e. the user supplied values for this column)
        // and the column is has "BY DEFAULT" as the identity generator
        // replace the SequenceNextValue is a IFNULL(<user value>, <sequence>) expression. 
        if (expression != null && 
                column.getDefaultIdentity() != null &&
                column.getDefaultIdentity().booleanValue()) { 
            seqExpr = generateIfNull (expression, seqExpr);
        }
        
        return seqExpr;
    }
    
    private TPreparedExpression generateIfNull(TPreparedExpression expr1, TPreparedExpression expr2 ) {
        List<TPreptimeValue> ifNullInput = new ArrayList<>(2);
        ifNullInput.add(new TNullExpression(expr1.resultType()).evaluateConstant(queryContext()));
        ifNullInput.add(new TNullExpression(expr2.resultType()).evaluateConstant(queryContext()));

        OverloadResult<TValidatedScalar> ifNullResult = registryService().getScalarsResolver().get("IFNULL", ifNullInput);
        TValidatedScalar ifNullOverload = ifNullResult.getOverload();
        List<TPreparedExpression> ifNullArgs = new ArrayList<>(2);
        ifNullArgs.add(expr1);
        ifNullArgs.add(expr2);
        return new TPreparedFunction(ifNullOverload, ifNullResult.getPickedInstance(),
                ifNullArgs, queryContext());
        
    }
}
