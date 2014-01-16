select animal.description, parent.name, child.name
FROM animal join parent on (animal.mother_id = parent.id)
    join child on (parent.id = child.pid)
WHERE animal.serial_number = '001'