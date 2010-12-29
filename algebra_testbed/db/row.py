class Row(object):

    def __init__(self, rowtype, keyvals):
        self._rowtype = rowtype
        for k, v in keyvals.iteritems():
            self.__dict__[k] = v
        self._key = [keyvals[field] for field in rowtype.key]

    def __repr__(self):
        value = ['%s: %s' % (field, self.__dict__[field])
                 for field in self._rowtype.value]
        if len(self._key) > 0:
            return '%s%s: %s' % (self._rowtype, self._key, '{%s}' % ', '.join(value))
        else:
            return '%s: %s' % (self._rowtype, '{%s}' % ', '.join(value))

    def __cmp__(self, other):
        return cmp(self._key, other._key)

    def __getitem__(self, field):
        return self.__dict__[field]

    def __setitem__(self, field, value):
        self.__dict__[field] = value

    rowtype = property(lambda self: self._rowtype)

    key = property(lambda self: self._key)

    hkey = property(lambda self: self['hkey'])

    def ancestor_of(self, other):
        ancestor = False
        self_key = self.hkey
        other_key = other.hkey
        if self_key and other_key and len(self_key) < len(other_key):
            ancestor = self_key == other_key[:len(self_key)]
        return ancestor
