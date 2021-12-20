What's new in Tarantool Data Grid 2.0
=====================================

Tarantool Data Grid (TDG) version 2.0 brings a lot of new features.
This document tells about all major changes and new capabilities in version 2.0:

.. contents::
    :local:
    :depth: 1

.. _tdg2wn-components:

Simplified architecture
-----------------------

Tarantool Data Grid 2.0 has four easily scalable components:

* ``Core``: configuration and administration
* ``Storage``: data validation and storage
* ``Runner``: running the business logic using Lua code
* ``Connector``: data exchange with external systems

You can add nodes instantly, with automatic data redistribution.
One TDG cluster consists of several replica sets.
In case one of the servers is down, the replica set keeps running without losing any data.

You can create as many replica sets with ``storage``, ``runner``, and ``connector`` components as you like.
The only exception is the ``core``.
There can be only one replica set that contains the ``core`` component.


Handlers instead of data pipelines
----------------------------------

When Tarantool Data Grid receives a data package from an external system,
it needs to process the data before putting it into a storage.

**In version 1.6**, data pipelines processed the incoming data by consecutively calling multiple functions.
However, writing code to build these pipelines was a bit of a challenge.

**In version 2.0**, there are handlers instead of pipelines.
Handlers are functions that process the incoming and outcoming data.
You can write a handler function using Lua and then bind it to any connector.

An input handler processes all incoming data.
After that, data objects are validated and put in a storage.
In case the validation process finishes with an error, objects are put in a repair queue.


Visual data model constructor
-----------------------------

Data model explicitly determines the structure of data.
All incoming data is verified, validated, and stored by the provided data model.

Tarantool Data Grid uses `Avro Schema <https://avro.apache.org/>`_ to describe data model.
**In version 2.0**, there is no need to write code to describe data structure.
Everything is done via an interface that is called a model editor.
Model editor has multiple options like adding a field, naming it, setting its type and value, as well as leaving comments.


Optional data versioning
------------------------

Data versioning allows one to trace what changes have been made to a data package and when.
Data package is a collection of data.

In Tarantool Data Grid, when you put a data package in a storage, it is identified by the primary index.
If an incoming package has the same primary index yet different data fields,
TDG will not delete the already stored data.
It will store the new data as a new version.

Version history often comes in handy, but it has one side effect.
The bigger version history is, the more it influences the performance of the storage.

**In TDG 2.0**, versioning is off by default.
It means that data packages with the same primary index will rewrite each other.
This increases performance and reduces the amount of space taken up by the database.
But if you need to keep version history, you can always switch this option on.


Multitenancy
------------

Tenant is a single team's workspace.
Due to multitenancy, several teams can use a single TDG instance and work on it independently.
Tarantool Data Grid helps isolate the code and data through a system of roles and permissions.
Each tenant has its own users.
Teams work separately, have no access to each other's data, and do not interfere with each other's processes.

**Perks:** you make the most of your hardware.
Also, TDG interface helps administer such a system by creating teams, roles and giving different rights to users.
