Setting up data actions
=======================

TDG allows you to set permissions for each user role to read and write data that is processed and stored in the system.
You can give users data access rights in the web interface by creating an access profile (data action) assigned to any of the user roles.

..  contents::
    :local:
    :depth: 1

..  _new-data-action:

Creating a new data action
--------------------------

To set up a new data action:

1.  Open the **Settings > Data actions** tab.

2.  Click **Add data action**.

3.  In the **New data action** dialog, set the data action's ``Name``
    and check the ``Read``/ ``Write`` rights for each aggregate:

..  image:: /_static/data-action-dialog.png
    :alt: New data action dialog

4.  Save the data action by clicking **Save**.

After creating a data action, you can edit any of its parameters.

..  _assign-data-action:

Assigning data actions to user roles
------------------------------------

You can assign data action to any user role created by the administrator.
However, assigning data actions to default roles is impossible, as they cannot be edited.

To assign a data action to a user role:

1.  Switch to the **Settings > Roles** tab.

2.  In the list of roles, choose the role you want to edit and click the pencil edit button.

3.  In the list of all actions, find **Data actions** section and tick the checkbox of the data action you want to assign to the role:

..  image:: /_static/assign-data-action.png
    :alt: Assigning data action to the user role

4.  Click **Apply**.

Likewise, you can assign data actions while creating a new role.
