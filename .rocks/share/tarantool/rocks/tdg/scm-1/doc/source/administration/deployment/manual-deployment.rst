First manual deployment
=======================

This guide explains how to quickly deploy Tarantool Data Grid (TDG) manually for the first time.
As a result, you will get a single node TDG cluster deployed locally.

..  note::

    For TDG deployment, you need Linux OS (CentOS 7/RHEL 7 are preferable).
    Otherwise, you'll need to set up a virtual machine with Linux OS first.


Getting a TGZ file for deployment
---------------------------------

To deploy Tarantool Data Grid, you need an RPM (``.rpm``), TGZ (``tar.gz``), or Docker image (``docker-image.tar.gz``) file.
For the first deployment, a TGZ file will do just fine.
It is easier to deploy and does not require root access.

Download a TGZ file of the latest version at the customer zone of `tarantool.io <https://www.tarantool.io/en/accounts/customer_zone/packages/tdg2>`_.
Make sure your browser did not unarchive the downloaded file: the file extension should be ``tar.gz``.

If you do not have access to the customer zone, you can get one by applying `this form <https://www.tarantool.io/en/datagrid/#contact>`_
or writing to **sales@tarantool.io**.

Deployment
----------

#.  Unpack ``tar.gz`` file:

    ..  code-block:: console

        $ tar xzf tdg-<VERSION>.tar.gz # change <VERSION> for the TDG version that you've downloaded

#.  Run a single node TDG cluster inside the unpacked ``tar.gz`` file:

    ..  code-block:: console

        $ ./tarantool ./init.lua --bootstrap true

    If you already have Tarantool installed, make sure that now, while deploying TDG,
    you use the Tarantool version that is packed in the just downloaded ``tar.gz`` archive.

#.  Go to `http://127.0.0.1:8080/ <http://127.0.0.1:8080/>`_ to check the deployed TDG:

    ..  image:: /_static/configured-instance.png
        :alt: Configured instance

    By running ``tarantool ./init.lua --bootstrap true``, you've deployed a configured instance with assigned roles.
    If you want to try and assign roles by yourself, run:

    ..  code-block:: console

        $ tarantool ./init.lua

    As a result, you'll get an unconfigured TDG instance:

    ..  image:: /_static/unconfigured-instance.png
        :alt: Unconfigured instance

    In case you want to start over and deploy TDG from scratch,
    don't forget to delete the configuration, xlog, and snap files that TDG created during the first deployment:

    ..  code-block:: console

        $ rm -rf ./dev/output

