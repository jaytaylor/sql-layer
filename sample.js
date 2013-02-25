// java.lang.System.out.println(dcontext);

var s = dcontext.createStatement();
var r = s.executeQuery("select * from customers");

// java.lang.System.out.println®;

var c = Packages.com.akiban.direct.entity.Test$Customer;
var x = r.getEntity(c);

// java.lang.System.out.println(x);

//r.next();
r.next();
x.name + " " + x.cid;
