# Of

Of is intended to be a simple query-object mapping library, for use in defining data models and database abstractions.

Instead of following the ORM pattern, Of provides contextual hints to traverse the model object graph via *queries*, working with fully formed query contexts.

Of holds the opinion that mapping individual tables is an *antipattern*; the object mentality of handling the authoritative representation of data is significantly different from relational models that any automated mapping will necessarily be flawed. Instead, Of lets the user flexibly manipulate how data is viewed and handled.

## Rendered Queries

Of uses Python's string formatting to render a query such as "SELECT * FROM users WHERE username = {self.user}" into a useful form, that is not vulnerable to injection attacks.
This makes it easier to write queries that interact with pythonic objects in a useful way; rendering out similarly to how an HTML template would be rendered.

## Supported Environments

Currently Of only supports Postgres through psycopg2. I have an idea on broader support, though.

# Usage

Of is intentionally simple, though somewhat more complex than a standard ORM.
    
    import of
    class myModel(of.Base):
        context = {myOtherModel: "SELECT * FROM myOtherTable WHERE something = {self.someitem}"}
        def othermodels(self):
            return of.conn(myOtherModel, self)

    class myOtherModel(of.Base):
        pass

    m = myModel({"id":1})
    m.othermodel()

And there you go.

# TODO

* Make 'of.Base' subclass `collections.MutableMapping` instead of object
* Multiple DB interface support
* Possibly have context traversal happen automagically.
* Cache rendered queries


# License

MIT; see the 'LICENSE' file.

# Contributors

* Aurynn Shaw <aurynn@gmail.com>