
Data access requests
====================

In this chapter, you will go through a number of use cases to understand the logic and syntax of data access requests in TDG.

You will use the already :doc:`deployed TDG cluster </administration/deployment/ansible-deployment>` as the environment to run requests.

..  contents::
    :local:
    :depth: 1


.. _graphql_model_prepare:

Preparing a data model
----------------------

To upload data in TDG and then access the data via GraphQL requests, you need to define a data model first.
You will use a simple model that has two object types—``Country`` and ``City``—with the following fields, indexes, and relations:

..  uml::

    skinparam monochrome true
    hide empty members

    abstract class Country << (B,white) >> {
    .. fields ..
    + title
    + phone_code
    .. indexes ..
    # title
    }

    abstract class City << (S,gray) >> {
    .. fields ..
    + title
    + country
    + population
    + capital
    .. indexes ..
    # primary [title, country]
    # title
    # country
    # population
    }

    Country "1" o-- "*" City

.. _graphql_model_avro:

Presenting a model in Avro Schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To upload the data model to TDG, you need to present it in the *Avro Schema* notation:

..  code-block:: js
    :emphasize-lines: 3, 15

    [
        {
            "name": "Country",
            "type": "record",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "phone_code", "type": ["null", "string"]}
            ],
            "indexes": ["title"],
            "relations": [
             { "name": "city", "to": "City", "count": "many", "from_fields": "title", "to_fields": "country" }
            ]
        },
        {
            "name": "City",
            "type": "record",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "country", "type": "string"},
                {"name": "population", "type": "int"},
                {"name": "capital", "type": "boolean"}
            ],
            "indexes": [
                {"name":"primary", "parts":["title", "country"]},
                "title",
                "country",
                "population"
            ]
        }
    ]

.. _graphql_model_upload:

Uploading the data model
------------------------

Next, you need to upload the :ref:`data model <graphql_model_avro>` to TDG. You can do so in the web interface.

1.  In a web browser, open the TDG web interface on an instance in a replica set with the "runner" cluster role.
    You can use the already :doc:`deployed TDG cluster </administration/deployment/ansible-deployment>`.
    In this case, the instance's URL will be `http://172.19.0.2:8082 <http://172.19.0.2:8082>`_.

2.  On the left menu, click the **Model** tab.
3.  Paste the :ref:`data model <graphql_model_avro>` into the **Request** field.

    ..  image:: /_static/model_upload02.png
        :alt: Data model upload

4.  Click **Submit**.

The data model has been uploaded. Now you can insert (upload), select, and delete data.

.. _graphql_data_upload:

Uploading data
--------------

You can upload data in TDG by means of a GraphQL mutation:

1.  On the left menu, click the **GraphQL** tab.
2.  Select **default** for the desired scheme and clear the request field.

    ..  image:: /_static/graphql.png
        :alt: GraphQL tab

3.  Paste the following request into the left field:

..  code-block:: graphql

    mutation all {
        russia:Country(insert: {
            title: "Russia",
            phone_code: "+7"}) {
        title
        phone_code
        }
        germany:Country(insert: {
            title: "Germany",
            phone_code: "+49"}) {
        title
        }
        moscow:City(insert: {
            title: "Moscow",
            country: "Russia",
            population: 12655050,
            capital: true}) {
        title
        country
        population
        capital
        }
        spb:City(insert: {
            title: "Saint Petersburg",
            country: "Russia",
            population: 5384342,
            capital: false}) {
        title
        country
        population
        capital
        }
        tver:City(insert: {
            title: "Tver",
            country: "Russia",
            population: 424969,
            capital: false}) {
        title
        country
        population
        capital
        }
        berlin:City(insert: {
            title: "Berlin",
            country: "Germany",
            population: 3520031,
            capital: true}) {
        title
        country
        population
        capital
        }
        munich:City(insert: {
            title: "Munich",
            country: "Germany",
            population: 1450381,
            capital: false}) {
        title
        country
        population
        capital
        }
        dresden:City(insert: {
            title: "Dresden",
            country: "Germany",
            population: 547172,
            capital: false}) {
        title
        country
        population
        capital
        }
    }

4.  Execute the mutation by clicking the **Execute Query** button:

    ..  image:: /_static/data_uploading02.png
        :alt: Uploading data

The data has been uploaded, as you can see by the system response in the right field.

.. _graphql_queries:

Data access requests
--------------------

Here are the common use cases for data access requests:

*   :ref:`General object type query <graphql_queries_gen>`
*   :ref:`Requests by primary index <graphql_queries_prim>`
*   :ref:`Requests by secondary index <graphql_queries_second>`
*   :ref:`Requests by compound index <graphql_queries_compound>`
*   :ref:`Comparison operators <graphql_queries_compar>`
*   :ref:`Multiple conditions <graphql_queries_multiple>`
*   :ref:`Requests by relations <graphql_queries_relations>`
*   :ref:`Pagination <graphql_queries_pagination>`
*   :ref:`Requests by version <graphql_queries_version>`

The easiest way to run GraphQL request examples is to use the embedded GraphiQL client in the TDG web interface.
For data access requests, use the **default** scheme:

1.  On the left menu, click the **GraphQL** tab.
2.  Select **default** for the desired scheme, clear the request field, and paste the example request code.

.. _graphql_queries_gen:

General object type query
~~~~~~~~~~~~~~~~~~~~~~~~~

To select objects of a particular type, specify the type's name and the object fields to return.
You don't have to indicate all the object fields that are defined in the data model. Specify any number of fields you need.
For example:

..  code-block:: graphql

    query {
      Country {
        title
      }
    }

The response is a JSON object that contains an array with all the records of the ``Country`` type.
For each record, the response includes only the fields specified in the request.

..  code-block:: json

    {
      "data": {
        "Country": [
          {
            "title": "Russia"
          },
          {
            "title": "Germany"
          }
        ]
      }
    }

.. _graphql_queries_prim:

Requests by primary index
~~~~~~~~~~~~~~~~~~~~~~~~~

A specific object can be selected by primary index:

..  code-block:: graphql

    query {
      Country(title: "Germany") {
        title
        phone_code
      }
    }

.. _graphql_queries_second:

Requests by secondary index
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Requests by secondary index have the same syntax:

..  code-block:: graphql

    query {
      City(country: "Russia") {
        title
        country
        population
      }
    }

.. _graphql_queries_compound:

Requests by compound index
~~~~~~~~~~~~~~~~~~~~~~~~~~

To perform a request by compound index, specify an array of field values:

..  code-block:: graphql

    query {
      City(primary: ["Saint Petersburg", "Russia"]) {
        title
        country
        population
      }
    }

.. _graphql_queries_compar:

Comparison operators
~~~~~~~~~~~~~~~~~~~~

Comparison operators are represented by index name suffixes.

Supported operators:

* ``_gt`` (Greater Than)
* ``_ge`` (Greater Than or Equal)
* ``_lt`` (Less Than)
* ``_le`` (Less Than or Equal)

For example:

..  code-block:: graphql

    query {
      City(population_ge: 1000000) {
        title
        country
        population
      }
    }

String field indexes support the ``_like`` operator so you can search for a particular pattern in a string.
You can use the wildcard sign ``%`` in the pattern.

..  code-block:: graphql

    query {
      City(title_like: "M%") {
        title
        country
      }
    }

.. _graphql_queries_multiple:

Multiple conditions
~~~~~~~~~~~~~~~~~~~

You can use several conditions in one request.
In this case, the request will search for objects satisfying all the conditions simultaneously (logical AND).
Use only indexed fields to specify the conditions.

..  code-block:: graphql

    query {
      City(country: "Russia", population_lt: 1000000) {
        title
        country
        population
      }
    }

.. _graphql_queries_relations:

Requests by relations
~~~~~~~~~~~~~~~~~~~~~

To select objects by relations, use the same syntax as in the general object type query.

In the :ref:`example model <graphql_model_prepare>`, there is a one-to-many relationship between the objects ``Country`` and ``City``.
Consequently, you can get the data both about the country and the cities in one request.

..  code-block:: graphql

    query {
        Country(title: "Russia") {
            title
            city {
                title
                population
        }
        }
    }

Response example:

..  code-block:: js

    {
      "data": {
        "Country": [
          {
            "title": "Russia",
            "city": [
              {
                "title": "Moscow",
                "population": 12655050
              },
              {
                "title": "Saint Petersburg",
                "population": 5384342
              },
              {
                "title": "Tver",
                "population": 424969
              }
            ]
          }
        ]
      }
    }

.. _graphql_queries_pagination:

Pagination
~~~~~~~~~~

TDG applies cursor-based pagination similar to the one described in the `GraphQL documentation <http://graphql.org/learn/pagination/#pagination-and-edges>`_.

In general, the request with pagination has the following syntax:

..  code-block:: graphql

    query {
        object_name(first:N, after:$cursor)
        }

where

*   ``first`` specifies the maximum number of elements to return. Defaults to 10.
*   ``after`` passes an opaque cursor---a string defining the element from which TDG should continue request execution.

Here is the first request with pagination:

..  code-block:: graphql

    query {
        City(first: 2) {
            title
            country
            cursor
        }
    }

The response is the following:

..  code-block:: js

    {
      "data": {
        "City": [
          {
            "cursor": "gaRzY2FukqZCZXJsaW6nR2VybWFueQ",
            "country": "Germany",
            "title": "Berlin"
          },
          {
            "cursor": "gaRzY2FukqdEcmVzZGVup0dlcm1hbnk",
            "country": "Germany",
            "title": "Dresden"
          }
        ]
      }
    }

To get the next data batch, take the ``cursor`` field's value of the last object received
and pass it as the ``after`` argument to the next request:

..  code-block:: graphql

    query {
        City(first: 2, after: "gaRzY2FukqdEcmVzZGVup0dlcm1hbnk") {
            title
            country
            cursor
        }
    }

Then run this logic in a cycle until you get an empty page:

..  code-block:: js

    {
      "data": {
        "City": []
      }
    }

Pagination for requests with relations works in a similar way:

..  code-block:: graphql

    query {
      Country(title: "Russia") {
        title
        city(first: 2) {
            title
            population
            cursor
        }
      }
    }

Reversed pagination is also possible: TDG returns objects preceding the element marked with a cursor.
For this, you need to specify a negative value for the ``first`` argument:

..  code-block:: graphql

    query {
        City(first: -2) {
            title
            country
            cursor
        }
    }

.. _graphql_queries_version:

Requests by version
~~~~~~~~~~~~~~~~~~~

TDG implements object versioning. Consequently, it can run requests by conditions that are based on object versions.
For more information, refer to the :doc:`/administration/versioning` page.
