select FATHER.name, MOTHER.name, animal.description
from parent AS FATHER, parent as MOTHER, animal
where animal.id = 42
AND FATHER.id = animal.father_id
AND animal.mother_id = MOTHER.id