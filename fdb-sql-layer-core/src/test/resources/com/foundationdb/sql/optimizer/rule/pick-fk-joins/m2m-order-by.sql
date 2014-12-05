-- Make sure that this is ordered correctly
SELECT parent.id as pid, parent.name, zoo1.zoo_id, animal.id as aid
FROM (SELECT zoo.id AS zoo_id FROM zoo) AS zoo1
JOIN animal ON zoo1.zoo_id = animal.zoo_id
JOIN parent ON parent.id = animal.mother_id
ORDER BY zoo1.zoo_id, animal.id
