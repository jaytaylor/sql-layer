
var s = dcontext.createStatement();
var r = s.executeQuery("select * from customers");
var c = Packages.com.akiban.direct.entity.Test$Customer;
var x = r.getEntity(c);
var result = "";
while (r.next()) {
  result += x.cid + " " +  x.name + '\n';
  for (o in Iterator( x.getOrderList())) {
    result += o.oid + " " + o.order_date + '\n';
  } 
}
result;