Mandatory authentication mode
=============================

Right after TDG deployment, any anonymous users and external applications can access all TDG functions and data.
As this action is not secure, TDG will highlight it in a warning message:

..  image:: /_static/warning-auth.png
    :alt: Warning message

To enable mandatory authentication mode:

1.  Create a :ref:`user profile <create-new-user>` with the "admin" role.

2.  Sign in to the system as this user.

3.  On the **Cluster** tab, enable the **Auth** toggle switch:

..  image:: /_static/enable-auth.png
    :alt: Enable authorization

4.  In the **Authorization** dialog, click **Enable**:

..  image:: /_static/enable-auth2.png
    :scale: 50%
    :alt: "Enable" button

The mandatory authentication mode is on.

Now users can access the TDG interface using login and password.
After :ref:`signing in <sign-in>`, they will have access to the tabs that are available to their :doc:`user role <role-access>`.
External applications can get authorized access to TDG data and functions via :doc:`tokens <tokens>`.
