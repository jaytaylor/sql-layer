package com.akiban.direct.schema;

import java.util.Date;
import java.util.List;
class Test {

    interface Address {

        int getAid();
        void setAid(int z1);

        int getCid();
        void setCid(int z1);

        String getState();
        void setState(String z1);

        String getCity();
        void setCity(String z1);

        Customer getCustomer();
    }

    interface Alpha {

        int getA();
        void setA(int z1);

        int getB();
        void setB(int z1);

        int getC();
        void setC(int z1);

        String getS();
        void setS(String z1);
    }

    interface Customer {

        int getCid();
        void setCid(int z1);

        String getName();
        void setName(String z1);

        List<Address> getAddress();

        List<Order> getOrder();
    }

    interface Item {

        int getIid();
        void setIid(int z1);

        int getOid();
        void setOid(int z1);

        String getSku();
        void setSku(String z1);

        int getQuan();
        void setQuan(int z1);

        Order getOrder();
    }

    interface Order {

        int getOid();
        void setOid(int z1);

        int getCid();
        void setCid(int z1);

        Date getOrder_date();
        void setOrder_date(Date z1);

        Customer getCustomer();

        List<Item> getItem();
    }
}
