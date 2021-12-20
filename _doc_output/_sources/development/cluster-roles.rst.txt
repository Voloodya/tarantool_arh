Understanding cluster roles
===========================

This chapter gives basic information about cluster roles in TDG.

The functions of TDG instances in the cluster are allocated based on roles.
Cluster roles are Lua modules that implement instance-specific logic.

TDG has four easily scalable cluster roles:

*   ``Core``: configuration and administration.
*   ``Storage``: data validation and storage.
*   ``Runner``: running the business logic using Lua code.
*   ``Connector``: data exchange with external systems.

There is also the cluster role called ``failover-coordinator`` for setting the stateful failover mode.
You can read more about this role in `Tarantool Cartridge documentation <https://www.tarantool.io/en/doc/latest/book/cartridge/cartridge_dev/#stateful-failover>`_.
