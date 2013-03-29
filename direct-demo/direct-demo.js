/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

function _register(registrar) {
	registrar.register(JSON.stringify(
			{"method":"GET","name":"totalComp","function":"computeTotalCompensation",
				"pathParams":"/<empno>", "queryParams":"start","in":"empno int required, start Date default `2000-01-01`","out":"String"}));
}

/*
 * Compute the total amount of compensation paid
 * to the specified employee by computing
 * rate * duration for each period of employment.
 */
function computeTotalCompensation(empno, start) {
	var emp = com.akiban.direct.Direct.context.extent.getEmployee(empno);
	
	var total = 0;
	var today = new Date();
	var summary = {from: today, to: today, total: total};

	for (var salary in Iterator(emp.salaries.sort("to_date"),where("to_date > " + start))) {
		var from = salary.fromDate;
		var to = salary.toDate.time < 0 
				? today : salary.toDate;
		if (from <summary > summary.to) {
			summary.to = to;
		}
		var duration = (to.getTime() - from.getTime()) / 86400000 / 365;
		summary.total += salary.salary * duration;
	}
	return JSON.stringify(summary);
}