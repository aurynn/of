import psycopg2
from psycopg2 import extras
from string import Formatter as f


def render (string, context):

    # Returns a string rendered against a context.
    # 
    pass

class wrapper(object):

    def __init__(self, query):
        self.query = query

class Pg(wrapper): pass

conn = None

def Connect(dict_):
    global conn 
    conn = Of(dict_)

class Of(object):
    """docstring for Of"""
    def __init__(self, params):
        # Where do the connection args come from?
        self.conn = psycopg2.connect(**params) # args here
        self.__queries = set()
        self.__stored = {}

    def transaction(self):
        # Returns a classable

        class tx(object):

            def __enter__(this):

                pass

            def __exit__(this):

                # Commit the TX
                self.conn.commit()

    def __call__(self, type_, caller, ctx=None):
        """
        Returns a given query's results mapped against the provided type.
        Context is the mechanism by which mapping happens. The Context is the 
        calling object; the type_ is the object which will wrap resulting rows.


        """

        kls = type_.__class__
        klsname = kls.__name__
        # print "type is %s" % type_
        # print "caller is %s" % caller.__class__

        # print "Type context is now %s" % type_.context
        # print "Type class is %s" % type_.__class__.__name__
        try:
            initial_query = caller.context[ type_ ]
        except KeyError as e:
            # print "Failed our key lookup for %s in %s" % (klsname, caller.__class__.__name__)
            try:
                initial_query = str(caller) # this is the alternative form
            except AttributeError:
                raise Exception("Missing context declaration in query context for class %s" % klsname)

        if ctx is None:
            ctx = {}

        ctx["self"] = caller

        return self._query(initial_query, ctx, type_)

    def _query (self, query, ctx, type_):

        # Next, render the query.
        # A given query is expected to have useful bits in.
        # 
        fo = f()
        new_query = ""
        args = {}
        selfobj = None
        if isinstance(ctx, Base):
            # Are we being called in a Self mode?
            selfobj = ctx
            ctx = { "self": ctx }
        # print "query is %s" % query

        for (text, key, spec, conversion ) in fo.parse( query ):
            # Text is the current bit lead up.
            # key is what we're going to need to replace.
            # print "text: %s, key: %s, spec: %s, conv: %s" % (text, key, spec, conversion)
            new_query += text
            try:
                # Use an empty list; we're not using positional args here.
                # print "101: key is %s " % key
                
                # print ctx["self"].username
                # (value, used) = fo.vformat(key, [], ctx)
                value = fo.vformat("{%s}" % key, [], ctx)
                # print "value is %s" % value
            except KeyError:
                # Well that's bad
                if not selfobj:
                    raise FormatError("Can't find %s in context of %s" % (key, type_))
                try:
                    # If we're being called in a 'self'-style context, we need to 
                    new_val = getattr(self, key)
                    (value, used) = fo.vformat(key, [], {key: new_val})
                except AttributeError:
                    raise FormatError("Can't find %s in context of %s" % (key, selfobj.__class__.__name__))

            new_query += """%(""" + key + """)s"""
            args[ key ] = value

        cur = self.conn.cursor(cursor_factory=extras.DictCursor)
        r = cur.execute(new_query, args)
        while 1:
            rows = cur.fetchmany(10)
            if not rows:
                return
            for row in rows:
                # print "Got a row, it's %s" % row
                return type_.from_db(row, ctx)

    def query(self, query, caller):
        """
        Directly runs a query using the caller as context.
        Does some basic wrapping around 
        """
        return self._query(query, caller)


class _Magic(type):

    """Magic metaclass! Removes the save method if you don't have a save thing."""

    def __new__(cls, name, bases, dict_):

        # If we haven't defined a way for our stuff to be updated at the DB 
        # layer, rip out the save function entirely.
        # print "got name %s" % name
        # print "got bases %s" % bases
        # print "got dict %s" % dict_
        if  dict_.get("__create__", None) or \
            dict_.get("__update__", None):

            # print "Adding save method"
            if dict_.get("save", None):
                # Well that's odd.
                pass
            dict_["save"] = _save # Re-bind the method
            # del dict_["save"]

        # x = super(_Magic, cls).__new__(cls, name, bases, dict_)
        x = type(name,bases,dict_)
        #x = type(name, bases, dict_)
        # print "In magic, classname is %s" % x.__class__.__name__
        return x

def _save(self):

    if self.__dict and self.__dirty__ and self._from_db:
        # Update query, if there was an originating dict and the 
        query = self.__update__
    elif self.__dirty__:
        query = self.__create__
    if isinstance(query, (list, tuple)):
        query = query[0] # Just use the first

    return of.query(query, self)

class Base(object):

    __metaclass__ = _Magic
    context = {}

    def __new__(cls, *args, **kwargs):

        obj = super(Base, cls).__new__(cls, *args, **kwargs)
        # print "Made an obj %s" % obj
        obj.__dirty__ = {}
        # obj.__dict__ = {}
        obj._from_db = False
        # print "Made an obj %s" % obj
        # print dir(obj)
        return obj

    def __init__(self, dict_, context=None):
        # print "Init!"
        # print "Dict is %s" % dict_
        self.__row__ = dict_
        self.__dirty__ = {}
        self.ctx = context

    def to_dict(self):
        if self.__dirty__:
            return dict(self.__dirty__)
        return dict(self.__row__)

    def __repr__(self):
        if self.__dirty__:
            return str(self.__dirty__)
        # return str(self.__row__)
        return str(dict(self.__row__))

    def __str__(self):
        return "Of model of type %s" % self.__class__.__name__

    @classmethod
    def from_db(cls, row, context):
        c = cls(row, context)
        object.__setattr__(c, "_from_db", True)
        # c._from_db = True
        # print "Row is %s" % c.__row__
        return c

    def __getattr__(self, key):

        __dirty__ = object.__getattribute__(self, "__dirty__")
        __row__ = object.__getattribute__(self, "__row__")
        if key in __dirty__:
            return __dirty__[key]

        return __row__[key]
        
    def __setattr__(self, key, value):
        
        if key in ["__row__", "__dirty__"]:
            # print "That's a paddlin'n."
            return object.__setattr__(self, key, value)
        try:
            if self.__dict__ and key in self.__dict__:
                self.__dirty__[key] = value
                return
        except AttributeError:
            return object.__setattr__(self, key, value)
        # return setattr(super(Base, self), key, value)
        return object.__setattr__(self, key, value)



class FormatError(BaseException): pass