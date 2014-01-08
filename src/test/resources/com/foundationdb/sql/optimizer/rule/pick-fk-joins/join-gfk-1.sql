select parent.name, child.name
from parent left join child on (parent.id = child.pid)
where parent.state = 'VT'