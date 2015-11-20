#!/usr/bin/env python2


"""
    >>> User = RecordSet("User", columns=("id", "name"), uniques=("id",))
    >>> Rows(User.id, User.name).append((1, "Joe"))
    >>> for user in User:
    ...     print user
    User(id=1, name='Joe')


    >>> User = RecordSet("User", columns=("id", "name"), uniques=("id",))
    >>> Rows(User.id, User.name).extend([(1, "Joe"), (2, "Sam")])
    >>> for user in User:
    ...     print user
    User(id=1, name='Joe')
    User(id=2, name='Sam')


    >>> User = RecordSet("User", columns=("id", "name"), uniques=("id",))
    >>> Rows(User.id, User.name).append((1, "Joe"))
    >>> Rows(User.id, User.name).extend([(1, "Joe"), (2, "Sam")])
    >>> for user in User:
    ...     print user
    User(id=1, name='Joe')
    User(id=2, name='Sam')


    >>> Article = RecordSet("Article", columns=("id", "title", "author_id"), uniques=("id",))
    >>> User = RecordSet("User", columns=("id", "name"), uniques=("id",))
    >>> Article.author = Query(Article).join(author_id=User.id).compile()
    >>> User.articles = Query(User).join(id=Article.author_id).compile()
    >>> Rows(Article.id,
    ...      Article.title,
    ...      Article.author_id|User.id,
    ...      User.name).extend(
    ...     [(1, "BREAKING NEWS", 1, "Joe"),
    ...      (2, "EXCLUSIVE", 2, "Sam")])
    >>> for article in Article:
    ...     print article, article.author
    Article(id=1, title='BREAKING NEWS', author_id=1) User(id=1, name='Joe')
    Article(id=2, title='EXCLUSIVE', author_id=2) User(id=2, name='Sam')
    >>> for user in User:
    ...     print user, user.articles
    User(id=1, name='Joe') [Article(id=1, title='BREAKING NEWS', author_id=1)]
    User(id=2, name='Sam') [Article(id=2, title='EXCLUSIVE', author_id=2)]
"""



class Record(object):
    __slots__ = ("_value",)


    def __init__(self, iterable):
        self._value = tuple(iterable)


    def __repr__(self):
        return self._repr_fmt % self._value


    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented

        return self._value == other._value


    def __getitem__(self, key):
        return self._value[key]



class RecordSet(type):


    def __new__(self, name, columns, uniques):
        attrs = {
            '__module__': self.__module__,
            '_uniques': uniques,
            '_records': [],
            '_indices': {col: {} for col in uniques},
            '_repr_fmt': '%s(%s)' % (name, ', '.join("%s=%%r"%c for c in columns)) }

        return type.__new__(self, name, (Record,), attrs)


    def __init__(self, name, columns, uniques):
        self._columns = []

        for name in columns:
            col = Column(self, name)
            self._columns.append(col)
            setattr(self, name, col)


    def __repr__(self):
        return "<RecordSet %s>" % self.__name__


    def __iter__(self):
        for record in self._records:
            yield record


    def __call__(self, value):
        new_record = type.__call__(self, value)
        unique_cols = [(col,getattr(new_record, col, None)) for col in self._uniques]

        if any(v is None for c, v in unique_cols):
            return None

        records = [self._indices[c].get(v, None) for c, v in unique_cols]

        if records and all(record==new_record for record in records):
            return records[0]

        assert all(record is None for record in records)

        self._records.append(new_record)

        for col, index in self._indices.items():
            v = getattr(new_record, col, None)
            if col in self._uniques:
                self._indices[col][v] = new_record
            else:
                l = self._indices[col].get(v, [])
                l.append(new_record)
                self._indices[col][v] = l

        return new_record



class Rows(object):


    def __init__(self, *columns):
        record_sets = set()
        column_indices = {}

        for i, col in enumerate(columns):
            if isinstance(col, Column):
                record_sets |= {col._set}
                column_indices[col] = i
            elif isinstance(col, ColumnSet):
                for c in col:
                    record_sets |= {c._set}
                    column_indices[c] = i

        self._column_maps = []

        for s in record_sets:
            indices = tuple(column_indices[c] for c in s._columns)
            self._column_maps += [(s, indices)]


    def append(self, obj):
        for record_set, column_map in self._column_maps:
            record_set(obj[index] for index in column_map)


    def extend(self, iterable):
        for t in iterable:
            self.append(t)



class DictRows(object):


    def __init__(self, **columns):
        record_sets = set()
        column_indices = {}

        for key, col in columns.items():
            if isinstance(col, Column):
                record_sets |= {col._set}
                column_indices[col] = key
            elif isinstance(col, ColumnSet):
                for c in col:
                    record_sets |= {c._set}
                    column_indices[c] = key

        self._column_maps = []

        for s in record_sets:
            indices = tuple(column_indices[c] for c in s._columns)
            self._column_maps += [(s, indices)]


    def append(self, obj):
        for record_set, column_map in self._column_maps:
            record_set(obj[index] for index in column_map)


    def extend(self, iterable):
        for t in iterable:
            self.append(t)



class Column(object):
    __slots__ = ('_set', '_name')


    def __init__(self, record_set, name):
        self._set = record_set
        self._name = name


    def __repr__(self):
        return "<Column %s.%s>" % (self._set.__name__, self._name)


    def __hash__(self):
        return hash((self._set, self._name))


    def __eq__(self, other):
        if not isinstance(other, Column):
            return NotImplemented

        return (self._set is other._set) and (self._name == other._name)


    def __get__(self, instance, owner):
        if instance is None:
            return self

        try:
            return instance[owner._columns.index(self)]
        except ValueError:
            raise AttributeError


    def __req__(self, other):
        return self.__eq__(other)


    def __or__(self, other):
        if not isinstance(other, Column):
            return NotImplemented

        return ColumnSet({self, other})


    def __getitem__(self, key):
        return self._set._indices[self._name][key]


    @property
    def is_unique(self):
        return self._name in self._set._uniques



class ColumnSet(frozenset):
    __slots__ = ()


    def __or__(self, other):
        if isinstance(other, Column):
            return ColumnSet(self | {other})
        elif isinstance(other, ColumnSet):
            return ColumnSet(self | other._columns)
        else:
            return NotImplemented



class RelationProperty(object):
    __slots__ = ("_joins",)


    def __init__(self, joins):
        self._joins = joins


    def __get__(self, instance, owner):
        if instance is None:
            return self

        many = False
        result = instance

        for l,r in self._joins:
            if many is False:
                result = r[getattr(result, l._name)]
                if not r.is_unique:
                    many = True

            elif many is True:
                result = [getattr(x, l._name) for x in result]

        return result



class Query(object):
    __slots__ = ("_set", "_joins")


    def __init__(self, record_set, joins=[]):
        self._set = record_set
        self._joins = joins


    def join(self, **kwargs):
        assert len(kwargs) == 1
        return Query(self._set, self._joins + kwargs.items())


    def compile(self):
        current = self._set
        joins = []

        for name, col in self._joins:
            joins.append((getattr(current, name), col))
            current = col._set

            if col._name not in current._indices:
                current._indices[col._name] = {}

        assert len([0 for l,r in joins if r.is_unique]) <= 1, "Too many manies"
        return RelationProperty(joins)
