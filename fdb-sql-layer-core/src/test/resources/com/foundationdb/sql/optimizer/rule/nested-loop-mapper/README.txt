left-join-one-sided-condition: left joins must push all conditions to nested part of join
left-join-one-sided-condition2: same as above but one-sided condition is on other table
anti-join-one-sided-condition: anti joins must push all conditions to nested part of join
anti-join-one-sided-condition2: same as above but one-sided condition is on other table

semi-join-one-sided-condition: semi joins should push conditions to outer table.
                               Condition should be pushed to select around referenced table.
semi-join-one-sided-condition2: same as above but on other table.
                                Condition should be pushed to select around referenced table.
inner-join-one-sided-condition: semi joins should push conditions to outer table.
                                Condition should be pushed to select around referenced table.
inner-join-one-sided-condition2: same as above but on other table.
                                 Condition should be pushed to select around referenced table.

