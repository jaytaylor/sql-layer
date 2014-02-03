select mammal.name
FROM mammal JOIN domesticanimal on (domesticanimal.mammal = mammal.animal)
    JOIN dog on (mammal.animal = dog.mammal)
where mammal.pregnant = 1