package com.akiban.server.service.restdml;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;

public class DeleteGenerator extends OperatorGenerator {

    public DeleteGenerator (AkibanInformationSchema ais) {
        super(ais);
    }
    
    @Override
    protected Operator create(TableName tableName) {
        Operator lookup = indexAncestorLookup(tableName); 
        // build delete operator.
        return API.delete_Returning(lookup, true, true);
    }       
    
}
