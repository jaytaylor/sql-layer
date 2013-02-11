package com.akiban.direct.schema;

import com.akiban.direct.schema.Test.Customer;
import com.akiban.qp.row.Row;
import com.akiban.server.types3.pvalue.PValueSource;

public class AddressImpl implements Test.Address {

    Row row;
    
    @Override
    public int getAid() {
        PValueSource source = row.pvalue(0);
        return source.getInt32();
    }

    @Override
    public void setAid(int z1) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int getCid() {
        PValueSource source = row.pvalue(1);
        return source.getInt32();
    }

    @Override
    public void setCid(int z1) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public String getState() {
        PValueSource source = row.pvalue(2);
        return source.getString();
    }

    @Override
    public void setState(String z1) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public String getCity() {
        PValueSource source = row.pvalue(3);
        return source.getString();
    }

    @Override
    public void setCity(String z1) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Customer getCustomer() {
        // TODO Auto-generated method stub
        return null;
    }

   

}
