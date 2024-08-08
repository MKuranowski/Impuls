impuls.multi\_file
==================

.. automodule:: impuls.multi_file
    :exclude-members: MultiFile

    .. Signature must be provided explicitly due to https://github.com/sphinx-doc/sphinx/issues/12695
        Seems that autodoc requires the signature to be on a single line.

    .. autoclass:: impuls.multi_file.MultiFile(options: PipelineOptions, intermediate_provider: IntermediateFeedProvider[ResourceT_co], intermediate_pipeline_tasks_factory: Callable[[IntermediateFeed[~impuls.LocalResource]], list[Task]], pre_merge_pipeline_tasks_factory: Callable[[IntermediateFeed[~impuls.LocalResource]], list[Task]] = empty_tasks_factory, final_pipeline_tasks_factory: Callable[[list[IntermediateFeed[~impuls.LocalResource]]], list[Task]] = empty_tasks_factory)
