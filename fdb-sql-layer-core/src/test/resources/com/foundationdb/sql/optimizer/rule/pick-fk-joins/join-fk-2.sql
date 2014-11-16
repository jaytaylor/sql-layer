select mammal.name
FROM mammal LEFT JOIN domesticanimal on (domesticanimal.mammal = mammal.animal)
    LEFT JOIN dog on (domesticanimal.mammal = dog.mammal)
where mammal.pregnant = 1