// java.lang.System.out.println(dcontext);


var s = dcontext.createStatement();
var r = s.executeQuery("select * from customers");
var c = Packages.com.akiban.direct.entity.Test$Customer;
var x = r.getEntity(c);
result = "";
while (r.next()) {
  result += x.cid + " " +  x.name + '\n';
}
result;