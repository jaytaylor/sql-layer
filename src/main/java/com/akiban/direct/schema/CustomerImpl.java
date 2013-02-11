package com.akiban.direct.schema;

import java.util.List;

import com.akiban.direct.schema.Test.Address;
import com.akiban.direct.schema.Test.Order;
import com.akiban.qp.row.Row;
import com.akiban.server.types3.pvalue.PValueSource;

public class CustomerImpl implements Test.Customer {

    Row row;

    @Override
    public int getCid() {
        PValueSource source = row.pvalue(0);
        return source.getInt32();
    }

    @Override
    public void setCid(int z1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        PValueSource source = row.pvalue(1);
        return source.getString();
    }

    @Override
    public void setName(String z1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Address> getAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Order> getOrder() {
        throw new UnsupportedOperationException();
    }

}
