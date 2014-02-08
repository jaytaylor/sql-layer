select mammal.name
FROM mammal LEFT OUTER JOIN domesticanimal on (domesticanimal.mammal = mammal.animal)
    LEFT OUTER JOIN dog on (mammal.animal = dog.mammal)
where mammal.pregnant = 1 AND domesticanimal.mammal IS NULL