
var s = dcontext.createStatement();
var r = s.executeQuery("select * from customers");
var c = Packages.com.akiban.direct.entity.Test$Customer;
var x = r.getEntity(c);
var copy;
var result = "";
var counter = 0;
while (r.next()) {
  result += x.cid + " " +  x.name + '\n';
  if (counter == 0) {
    copy = x.copy();
    java.lang.System.out.println("Copied: " + x + " to " + copy + " counter=" + counter + " copy.cid=" + copy.cid);
  }
  counter++;
}
result += "copied value: " + copy.cid + " " +  copy.name + '\n';
result;