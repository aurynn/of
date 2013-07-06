import psycopg2
from psycopg2 import extras
from string import Formatter as f
import types

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
        psycopg2.extras.register_hstore(self.conn, globally=True)
        self.__queries = set()
        self.__stored = {}

    def transaction(self):
        # Returns a classable

        class tx(object):

            def __enter__(this):

                pass

            def __exit__(this, type, value, traceback):

                # Commit the TX
                if traceback:
                    self.conn.rollback()
                    return
                self.conn.commit()
                return
        return tx()

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

    def _query (self, query, ctx, type_=None):

        # Next, render the query.
        # A given query is expected to have useful bits in.
        # 

        # Use a dict by default
        if type_ is None:
            type_ = dict

        fo = f()
        new_query = ""
        args = {}
        selfobj = None
        if isinstance(ctx, Base):
            # Are we being called in a Self mode?
            # print "in self mode"
            selfobj = ctx
            ctx = { "self": ctx }
        # print "query is %s" % query
            # print ctx["self"].__row__
        for (text, key, spec, conversion ) in fo.parse( query ):
            # Text is the current bit lead up.
            # key is what we're going to need to replace.
            # print "text: %s, key: %s, spec: %s, conv: %s" % (text, key, spec, conversion)
            new_query += text
            if key is None:
                continue
            value = None
            try:
                # Use an empty list; we're not using positional args here.
                # print "101: key is %s " % key
                # (value, used) = fo.vformat(key, [], ctx)
                # value = fo.vformat("{%s}" % key, [], ctx)
                (value, used_key) = fo.get_field(key, [], ctx)
            except KeyError as excepted:
                # Well that's bad
                if not selfobj:
                    raise FormatError("Can't find %s in context of %s" % (key, type_))
                try:
                    # If we're being called in a 'self'-style context, we need to 
                    # This is also terribly wrong, wtf
                    # new_val = getattr(ctx["self"], key)
                    try:
                        ctx["self"]
                        (value, used_key) = fo.get_field(key, [], ctx["self"])
                    except Exception, e:
                        print "Couldn't find self in ctx"
                        raise excepted
                except AttributeError:
                    raise FormatError("Can't find %s in context of %s" % (key, selfobj.__class__.__name__))

            new_query += """%(""" + key + """)s"""
            args[ key ] = value

        cur = self.conn.cursor(cursor_factory=extras.DictCursor)
        # print new_query
        # print args
        r = cur.execute(new_query, args)
        if r:
            raise psycopg2.ProgrammingError("query error: %s" % r)
        while 1:
            try:
                rows = cur.fetchmany()
                if not rows:
                    break
                for row in rows:
                    # print "Got a row, it's %s" % row
                    t = type_(row, ctx)
                    t._from_db = True
                    yield t
                    # yield type_.from_db(row, ctx)
            except psycopg2.ProgrammingError as e:
                return # return NOTHING.
        return
                

    def query(self, query, caller):
        """
        Directly runs a query using the caller as context.
        Does some basic wrapping around 
        """
        return self._query(query=query, ctx=caller)


class _Magic(type):

    """Magic metaclass! Removes the save method if you don't have a save thing."""

    def __new__(cls, name, bases, dict_):

        # If we haven't defined a way for our stuff to be updated at the DB 
        # layer, rip out the save function entirely.
        # print "got name %s" % name
        # print "got bases %s" % bases
        # print "got dict %s" % dict_
        if "__create__" in dict_ or "__update__" in dict_:
            # print "Adding save method"
            if dict_.get("save", None):
                # Well that's odd.
                print "WELL THAT'S ODD"
            else:
                dict_["save"] = _save # Re-bind the method

        if "__fetch__" in dict_:
            dict_["fetch"] = _fetch

        x = super(_Magic, cls).__new__(cls, name, bases, dict_)
        # x = type(name, bases, dict_)
        #x = type(name, bases, dict_)
        # print "In magic, classname is %s" % x.__class__.__name__
        return x

def _save(self):

    query = None
    querytype = None
    print "update: %s" % self.__update__
    print "create: %s" % self.__create__
    if self.__row__ and self.__dirty__ and self._from_db:
        # Update query, if there was an originating dict and the 
        querytype = 1
        query = self.__update__
    elif not self._from_db and self.__row__:
        querytype = 0
        query = self.__create__
    elif self.__dirty__:
        querytype = 0
        query = self.__create__

    if isinstance(query, (list, tuple)):
        query = query[0] # Just use the first

    if not query:
        raise AttributeError("save is not defined")

    try:
        gen = conn.query(query, self)
    except psycopg2.IntegrityError as e:
        # were we using the create query?
        if not querytype:
            raise
        query = self.__update__
        gen = conn.query(query, self)
    if not isinstance(gen, types.GeneratorType):
        return gen
    # otherwise
    try:
        for i in gen:
            return i
    except psycopg2.IntegrityError:
        print "FAILURE"
        if querytype:
            raise
        print "nq now %s" % self.__update__
        nq = self.__update__
        # Roll back
        conn.conn.rollback()
        for i in conn.query(nq, self):
            conn.conn.commit()
            return i

def _fetch(self):
    """Populates the object with data from the DB.
       Requires a load query.
       If only there was, you know, a query generator here.
    """
    dct = self.__dirty__ or self.__row__
    query = self.__fetch__
    query += " WHERE "
    args = []
    for key in dct.keys():
        q = "%s = " % key
        q += "{self." + key + "}" # Use the existing lookup stuff, since these are already in self.
        args.append(q)
    
    query += " AND ".join(args) # whee

    res = conn.query(query, self)
    # res is a generator
    this = None # This becomes the new core of our data
    try:
        for this in res: 
            break # One pass only
    except TypeError:
        return False

    if this and dict( this ):
        self.__row__ = dict( this )
        self._from_db = True # We're now known to be from the DB
        conn.conn.rollback()
        return True
    else:
        return False

class Base(object):

    __metaclass__ = _Magic
    context = {}

    def __new__(cls, *args, **kwargs):

        obj = super(Base, cls).__new__(cls, *args, **kwargs)
        obj.__dirty__ = {}
        obj.__row__ = {}
        obj._from_db = False
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
        dct = None
        if self.__dirty__:
            dct = self.__dirty__
        else:
            dct = self.__row__
        return "Of %s: %s" (self.__class__.__name__, str(dct))

    @classmethod
    def from_db(cls, row, context):
        c = cls(row, context)
        object.__setattr__(c, "_from_db", True)
        # c._from_db = True
        # print "Row is %s" % c.__row__
        return c

    def __getitem__(self, key):

        # __dirty__ = object.__getattribute__(self, "__dirty__")
        # __row__ = object.__getattribute__(self, "__row__")
        if key in self.__dirty__:
            return self.__dirty__[key]
        else:
            return self.__row__[key]

    def __getattr__(self, key):
        try:
            dirty = object.__getattribute__(self, "__dirty__")
        except AttributeError:
            dirty = {}
        try:
            row = object.__getattribute__(self, "__row__")
        except AttributeError:
            row = {}
        if key == "__dirty__":
            return dirty
        if key == "__row__":
            return row
        if key in dirty:
            return dirty[key]
        elif key in row:
            return row[key]

        raise KeyError("%s not in me" % key)

        
    def __setitem__(self, key, value):
        
        self.__dirty__[key] = value

        # if key in ["__row__", "__dirty__"]:
        #     # print "That's a paddlin'n."
        #     return object.__setattr__(self, key, value)
        # try:
        #     if self.__dict__ and key in self.__dict__:
        #         self.__dirty__[key] = value
        #         return
        #     else:
        #         self.__dirty__[key] = value
        #         return
        # except AttributeError:
        #     return object.__setattr__(self, key, value)
        # # return setattr(super(Base, self), key, value)
        # return object.__setattr__(self, key, value)


class FormatError(BaseException): pass
