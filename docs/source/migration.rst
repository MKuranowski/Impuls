Migrating Major Versions
========================

v1 to v2
--------

2.0.0 brings changes to the DB schema to allow generic storage of custom fields
on existing entities and generic storage of custom tables through the
:py:class:`~impuls.model.ExtraFieldsMixin` and :py:class:`~impuls.model.ExtraTableRow` classes.
This brought a slight change to the way :py:class:`~impuls.tasks.LoadGTFS` and
:py:class:`~impuls.tasks.SaveGTFS` tasks work.

Additionally, ``Stop.pkpplk_code``, ``Stop.ibnr_code`` and ``StopTime.original_stop_id`` attributes
were removed.

To migrate:

1. Remove any ``.db`` files from workspace folders, this can be simply done by removing the entire workspace directory.
2. Add ``.txt`` suffixes to headers provided to the :py:class:`~impuls.tasks.SaveGTFS` task.
3. If using any of the removed fields, adjust code to use the generic :py:class:`~impuls.models.ExtraFieldsMixin`
   dictionary. Note that to load those fields from GTFS, :py:attr:`LoadGTFS.extra_fields <impuls.tasks.LoadGTFS.extra_fields>`
   must be set to ``True``.

All else stays unchanged.
