Repair queue
============

If TDG cannot process an incoming object, the system puts the object in the repair queue.
The administrator then checks it, fixes the problem, and sends the object to be processed again.

..  _repair-queue-input:

Input
-----

On the :guilabel:`Repair queues` > :guilabel:`Input` tab, there is a repair queue for submitted objects.

..  image:: /_static/input.png
    :alt: Repair queues > Input

Here you can find the incoming objects that TDG could not process.
Here are the main reasons why objects end up in the repair queue:

*   Error when processing an incoming object with a handler.

*   The TDG system expects an object in a particular format,
    but the incoming object from an external system was submitted in a different format.

*   Internal system error.

*   Hardware failure.

You can work with objects in the repair queue through the web interface on the :guilabel:`Input` tab:

..  image:: /_static/input-list.png
    :alt: Objects in the repair queue

On this tab, you can see the list of objects, their status ("New", "In Progress", "Reworked"),
and the date and time when they were placed in the repair queue.

The :guilabel:`Repair queue` interface lets you do the following:

*   :guilabel:`Filter`: filter the objects by any combination of characters in any table column or by specifying the date or time range.

*   :guilabel:`Try again`: process the object again by the same handler.

*   :guilabel:`Delete`: delete the selected object from the repair queue.

*   :guilabel:`Try again all`: process all objects one more time.

*   :guilabel:`Delete all`: delete all objects from the repair queue.

Click on the object to see its details:

..  image:: /_static/object-info.png
    :alt: Detailed information about the object

In the :guilabel:`Object info` dialog, you can see:

*   :guilabel:`ID`: the object's UUID.

*   :guilabel:`Reason`: error description and the complete stack trace.

*   :guilabel:`Object`: the object's current structure in the JSON format.

When an object gets into the repair queue, it has the status "New".
When it is processed for a second time, the object's status changes to "In Progress".
If the object was processed successfully, it is removed from the repair queue.
If an error occurs during reprocessing,
the system will display an error message.
The object will remain in the repair queue with the status "Reworked".

Notifications
~~~~~~~~~~~~~

TDG users can receive notifications in case an object gets into the repair queue.
To enable notifications, you need a mail server and a list of subscribers---that is, recipients.

On the :guilabel:`Settings` > :guilabel:`Mail server` tab, set the following parameters:

*   :guilabel:`Url`: the SMTP server used to send notifications.

*   :guilabel:`From`: the sender that will be shown in the mail client.

*   :guilabel:`Username`: SMTP server user name.

*   :guilabel:`Password`: SMTP server user password.

*   :guilabel:`Timeout (sec)`: SMTP server request timeout in seconds.

On the :guilabel:`Settings` > :guilabel:`Subscribers` tab, click the :guilabel:`Add subscriber` button to add a new subscriber.
Specify the subscriber's name and email.
Later, you can edit the subscriber's profile or delete it.

..  _repair-queue-output:

Output
------

The object replication mechanism allows you to send objects to external systems in the desired format.
In case of an error during the replication process, the object gets in the replication repair queue on the :guilabel:`Output` tab.

This queue has the same functions as the :ref:`repair queue <repair-queue-input>` on the :guilabel:`Input` tab.
The only difference is that the repair queue on the :guilabel:`Input` tab is for submitted objects that could not be processed and saved,
whereas the replication repair queue on the :guilabel:`Output` tab is for objects that could not be replicated.

To work with objects in the replication repair queue, open the :guilabel:`Repair queues` > :guilabel:`Output` tab:

..  image:: /_static/output.png
    :alt: Replication repair queue

Like in the :ref:`repair queue <repair-queue-input>`, you can filter objects, delete them, and try to replicate them again.

The object status shows the reason why the object ended up in the replication repair queue:

*   "Preprocessing error": the replicated object was processed with an error.

*   "Sending error": an error occured while sending the object to an external system.

If you choose an object and click :guilabel:`Try again`, the object will be processed again.
Its status will change from "New" to "In progress".
If the operation is successful, the object will be moved to the next stage or deleted from the repair queue.
If the operation finishes with an error, the status will change to "Rereplicated (Preprocessing error)" or "Rereplicated (Sending error)".
The object will remain in the replication repair queue.

..  _repair-queue-jobs:

Jobs
----

This is a repair queue for pending jobs that ended with an error.
To monitor these jobs, open the :guilabel:`Repair queues` > :guilabel:`Jobs` tab:

..  image:: /_static/jobs.png
    :alt: Repair queue for pending jobs

This tab has the same functions as the :ref:`repair queue for submitted objects <repair-queue-input>` on the :guilabel:`Input` tab.
