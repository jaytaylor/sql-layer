COIA without group index

select-1: index condition on one branch, all columns from another branch

select-2: index condition on one branch, columns also from another branch

select-3: index condition on one branch, all columns from another branch, 
          including primay key

select-4: index condition on one branch, several levels from another branch

select-5: columns from two branches below indexed table

select-6: ordering index and column from a second branch

select-7: ordering index and two levels from a second branch

select-8: group scan with multiple branches (former fail)

select-9a: scan with WHERE 1

select-9b: scan with WHERE 0
