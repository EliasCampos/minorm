MinORM
======

A minimalistic ORM with basic features.

Inspired by Django ORM.

What's the point?
-----------------
MinORM was designed as minimalistic ORM, to be as simple as possible.
It's not production-ready solution, rather a proof of concept. The goal is to demonstrate example of an ORM,
more-less applicable for usage, that could be created with python in a short term of time.

Usage
-----
DB Connection
*************

Establish connection to database by calling :code:`.connect()` method of :code:`connector` object, with certain db handler.

Connecting to sqlite database:

.. code:: python

    from minorm.connectors import connector
    from minorm.db_specs import SQLiteSpec

    connector.connect(SQLiteSpec('example.db'))

Connecting to postgresql database (requires psycopg2 to be installed):

.. code:: python

    from minorm.connectors import connector
    from minorm.db_specs import PostgreSQLSpec

    connection_string = "host=localhost port=5432 dbname=mydb user=admin password=secret"
    connector.connect(PostgreSQLSpec(connection_string))

Close connection by calling :code:`.disconnect()` method:

.. code:: python

    connector.disconnect()

Models
******

Create a model class that represents a single table in a database:

.. code:: python

    from minorm.models import Model
    from minorm.fields import CharField, IntegerField

    class Person(Model):
        name = CharField(max_length=120)
        age = IntegerField()

It's possible to create a new table in a database:

.. code:: python

    Person.create_table()

Or to use existing one, by set table name in model meta:

.. code:: python

    class Book(Model):
        title = CharField(max_length=90)

        class Meta:
            table_name = "some_table"

It's possible to drop a table:

.. code:: python

    Person.drop_table()

Create a new instance or update existing one in db by calling :code:`save` method:

.. code:: python

    person = Person()
    person.name = "John" # set field values as attributes
    person.age = 33
    person.save()

    book = Book(title="foobar")  # or pass it in init method
    book.save()

Remove a row from db by calling :code:`delete` method:

.. code:: python

    person.delete()

Create a model with foreign relation by using :code:`ForeignKey` field:

.. code:: python

    class Book(Model):
        title = CharField(max_length=90)
        author = ForeignKey(Person)

Pass an instance of related model when saving a new one:

.. code:: python

    book = Book(title="foobar", author=person)
    book.save()

Queryset methods
****************
Use queryset, accessible by model's :code:`qs` property, to perform db operations on multiple rows:

:code:`filter(**lookups)`:
    Filter query, result will contain only items that matches all lookups:

    .. code:: python

        # user type is "member" AND age > 18
        filtered_qs = Person.qs.filter(user_type='member', age__gt=18)

    List of supported lookup expressions:

    - :code:`lt`, :code:`lte` - less than (or equal)
    - :code:`gt`, :code:`gte` - greater than (or equal)
    - :code:`neq` - not equal
    - :code:`in` - checks if value is between given options
    - :code:`startswith`, :code:`endswith`, :code:`contains` - check inclusion of a string

    It's also possible to filter by foreign relation fields:

    .. code:: python

        qs = Book.qs.filter(author__name="Mark Twain")  # will perform join of `author` table


:code:`aswell(**lookups)`:
    Make query result to include items that also matches lookups listed in the method:

    .. code:: python

        # age > 18 OR user is admin
        filtered_qs = Person.qs.filter(age__gt=18).aswell(user_type="admin")

:code:`order_by(*fields)`:
    Set ordering of queried rows. Use :code:`-` prefix to reverse order:

    .. code:: python

        Book.qs.order_by('created')  # for oldest to newest
        Person.qs.order_by('-id')  # reverse ordering by id

Slicing (limit number of row):
    it's possible to limit number of selected rows by using slices:

    .. code:: python

        persons = Person.qs[:3]  # will limit results number to 3 items


:code:`all()`:
    Get a copy of the queryset:

    .. code:: python

        qs = Person.qs.filter(age=42)
        new_qs = qs.all()  # a copy of filtered qs

:code:`values(*fields)`:
    Prepare qs to get rows as dictionaries with fields, passed to the method:

    .. code:: python

        qs = Book.qs.values('title', 'author__name')  # items will be dicts with this two keys

:code:`exists()`:
    Return boolean, that indicates presence of rows that match filters:

    .. code:: python

        Person.qs.filter(name="mike").exists()  # True if there is such name, otherwise False
        Book.qs.exists()  # check if there is at least one row in the table

:code:`get(**lookups)`:
    Get single row as an instance of the model class:

    .. code:: python

        person = Person.qs.get(id=7)  # model instance object
        book = Book.qs.get(pk=7)  # you could use `pk` instead of pk field name

    raises :code:`Model.DoesNotExists` if corresponding row not found in db,
    and :code:`MultipleQueryResult` if more than one row matches query filters.

:code:`fetch()`:
    Get all rows as a list of namedtuple objects:

    .. code:: python

        persons = Person.qs.fetch()  # list of namedtuples
        adults = Person.qs.filter(age__gte=18).fetch()

Iterating queryset:
    Queryset supports iterator interface, so it's possible to iterate results:

    .. code:: python

        for adult in Persons.qs.filter(age__gte=18):
            print(adult.pk, adult.name)  # each item is a model instance


:code:`create(**field_values)`:
    Create a new instance in db:

    .. code:: python

        person = Person.qs.create(name="John", age=33)

    is a shortcut for two calls:

    .. code:: python

        person = Person(name="John", age=33)
        person.save()

:code:`update(**field_values)`:
    Update field values of existing rows in db:

    .. code:: python

        Book.qs.filter(price__lt=200).update(price=250)


:code:`delete()`:
    Remove all rows of queryset from db:

    .. code:: python

        Product.qs.filter(created__lt=date(2020, 11, 10)).delete()

:code:`bulk_create(instances)`:
    Create multiple instances in one db query:

    .. code:: python

        Book.qs.bulk_create([
            Book(title="foo", author=1),
            Book(title="bar", author=2),
            Book(title="baz", author=1),
        ])  # creates all these books in one query


:code:`select_related(*fk_fields)`:
    Prepare queryset to perform select query with join of foreign relation:

    .. code:: python

        for book in Book.qs.select_related('author'):
            # without select_related call, each related object hits db
            author = book.author
            print(book.title, author.name)

Transactions support
********************
It's possible to perform multiple model/queryset operations in transaction by using `transaction` module:

.. code:: python

    from minorm import transaction

    with transaction.atomic():
        # all db operations inside `atomic` block will run in one transaction
        author = Person.objects.create(name="Steven King", age=19)
        Book.objects.create(title="The Dark Tower: The Gunslinger", author=author)

It's also possible to manually commit/rollback changes inside transaction block:

.. code:: python

    with transaction.atomic():
        instance.save()  # instance is set for saving in transaction
        if want_to_keep:
            transaction.commit()  # permanently save instance in db
        else:
            transaction.rollback()  # remove instance from saving

        # do more stuff if it's required


TODO
----
* add more model fields
* test Postgresql support
* add basic aggregation functions (SUM, COUNT, etc)

Running tests
-------------
To run tests, create virtual environment and then run:

.. code:: bash

    make test

License
-------
The MIT License (MIT)


Contributed by `Campos Ilya`_

.. _Campos Ilya: https://github.com/EliasCampos