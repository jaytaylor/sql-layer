
var c;
var result = "";
for (c in Iterator(dcontext.extent.getCustomerList().where("cid = 1"))) {
  result += "customer: " + c.cid + " " +  c.name + '\n';
  for (o in Iterator(c.orderList)) {
    result += " order: " + o.oid + " " + o.order_date + '\n';
    var p = o.customer;
    result += "   parent's customer: " + p.cid + " " +  p.name + '\n';
  } 
}

result;