/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 * 
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Compute the total amount of compensation paid to the specified employee
 * between dates start and end by computing (rate * duration) for each period of
 * employment.
 * 
 * The following distinguished comments define REST end-point and map them to functions
 * defined below.
 */

function _register(registrar) {
        registrar.register("method=GET path=totalComp/(\\\\d*) "
                        + "function=computeTotalCompensation "
                        + "in=(pp:1 int required, qp:start Date default '1800-01-01', "
                        + "qp:end Date default TODAY), out=string");

        registrar.register("method=POST path=raiseSalary "
                        + "function=raiseSalary "
                        + "in=(json:empid int required, json:percent double required, "
                        + "json:effectiveDate Date required) out=string");

}

function computeTotalCompensation(empno, start, end) {
	var extent = com.foundationdb.direct.Direct.context.extent;
	var emp = extent.getEmployee(empno);
	var newSalary = emp.salaries.newInstance();
	println("Computing total compensation for employee " + empno);
	var total = 0;
	var today = new Date();
	var summary = {
		from : today,
		to : new Date(0),
		total : 0
	};

        var it = (typeof Iterator == 'undefined') ?
            ( function (x) { return x; } ) :
            ( function (x) { return Iterator(x.iterator()); } );

	for each ( var salary in it(emp.salaries.sort("to_date"))) {
		var from = new Date(salary.fromDate.time);
		if (from < summary.from) {
			summary.from = from;
		}
		var to = salary.toDate.time < 0 ? today : new Date(salary.toDate.time);
		if (to > summary.to) {
			summary.to = to;
		}
		var earliest = Math.max(start.getTime(), from.getTime());
		var latest = Math.min(end.getTime(), to.getTime());
		var duration = (latest - earliest) / 86400000 / 365;
		if (duration > 0) {
			summary.total += salary.salary * duration;
		}
	}
	return JSON.stringify(summary);
}

function raiseSalary(empno, percent, effectiveDate) {

	var emp = com.foundationdb.direct.Direct.context.extent.getEmployee(empno);
	var multiplier = 1.0 + (percent / 100.0);
	println("Multiplier is " + multiplier);
	var iter = emp.salaries.sort("to_date", "desc").iterator();
	var salary;
	if (iter.hasNext()) {
		salary = iter.next();
	}
	if (salary == null) {
		println("Employee " + emp.lastName + " has no salary records");
		return;
	}
	var newSalary = salary.salary * multiplier;
	println("Salary for employee " + emp.lastName + " would be raised to "
			+ newSalary + " on " + effectiveDate
			+ " if Direct had update support");

}

function println(s) {
	java.lang.System.out.println(s);
}
