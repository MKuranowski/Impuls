# © Copyright 2022-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Iterable, TypeVar

_T = TypeVar("_T")


class InputNotModified(Exception):
    """InputNotModified is raised by the Pipeline when no resources have changed,
    preventing pointless processing of the same data."""

    pass


class DataError(ValueError):
    """DataError represents any error related to incorrect input data.

    The main point of DataErrors is that it may be caught
    and any underlying process may continue. Thus, any process raising
    DataError must not leave the pipeline in an undefined state.
    """

    pass


class ResourceNotCached(DataError):
    """ResourceNotCached is raised by the Pipeline run with the from_cache option on
    when a Resource is not available locally.
    """

    resource_name: str

    def __init__(self, resource_name: str) -> None:
        self.resource_name = resource_name
        super().__init__(f"Resource is not cached: {resource_name}")


class MultipleDataErrors(DataError):
    # FIXME: Move to ExceptionGroup when support for 3.10 is dropped

    """MultipleDataErrors is raised when a process encounters a non-zero amount of DataErrors.

    For most use cases the catch_all helper can be used to catch any DataErrors
    that might be encountered.
    """

    errors: list[DataError]

    def __init__(self, when: str, errors: list[DataError]) -> None:
        self.errors = errors
        super().__init__(
            f"{len(errors)} error(s) encountered during {when}:\n    "
            + "\n    ".join(err.args[0] for err in errors)
        )

    @classmethod
    def catch_all(
        cls,
        context: str,
        may_raise_data_error: Iterable[_T],
        /,
        flatten: bool = False,
        deduplicate: bool = False,
    ) -> list[_T]:
        """catch_all takes a generator that may raise DataError when retrieving
        the next item; and catches all the DataErrors to raise a single
        MultipleDataErrors once the generator is exhausted.

        If no DataErrors were thrown, returns all non-None items
        from the generator.

        Any other Exception is passed through to the caller.

        Note that ``may_raise_data_error`` must not be a generator expression,
        and should usually be a ``map`` object. Generator expressions stop at the
        first raised exception, making the whole endeavor useless.

        If ``flatten`` or ``deduplicate`` are set to True, the corresponding
        action is run on the MultipleDataErrors object before it is raised.
        If both are specified, flatten is run first.

        >>> def some_function(x: int) -> int:
        ...    if x % 5 == 0:
        ...        raise DataError(f"Oh no, got {x}")
        ...    return x
        >>> MultipleDataErrors.catch_all("foo", map(some_function, range(1, 5)))
        [1, 2, 3, 4]
        >>> MultipleDataErrors.catch_all("foo", map(some_function, range(1, 6)))
        Traceback (most recent call last):
        ...
        impuls.errors.MultipleDataErrors: 1 error(s) encountered during foo:
            Oh no, got 5
        >>> MultipleDataErrors.catch_all("foo", map(some_function, range(1, 11)))
        Traceback (most recent call last):
        ...
        impuls.errors.MultipleDataErrors: 2 error(s) encountered during foo:
            Oh no, got 5
            Oh no, got 10
        """
        it = iter(may_raise_data_error)
        errors: list[DataError] = []
        elements: list[_T] = []
        done: bool = False

        while not done:
            try:
                element = next(it)
                if element is not None:
                    elements.append(element)
            except StopIteration:
                done = True
            except DataError as e:
                errors.append(e)

        if errors:
            if flatten:
                errors = cls.flatten(errors)
            if deduplicate:
                errors = cls.deduplicate(errors)
            raise MultipleDataErrors(context, errors)

        return elements

    @staticmethod
    def flatten(errors: Iterable[DataError]) -> list[DataError]:
        """flatten recursively flattens any nested MultipleDataErrors.

        >>> MultipleDataErrors.flatten([
        ...    DataError("foo"),
        ...    MultipleDataErrors("test2", [DataError("bar")]),
        ... ])
        [DataError('foo'), DataError('bar')]
        """
        flat: list[DataError] = []
        for err in errors:
            if isinstance(err, MultipleDataErrors):
                flat.extend(MultipleDataErrors.flatten(err.errors))
            else:
                flat.append(err)
        return flat

    @staticmethod
    def deduplicate(errors: Iterable[DataError]) -> list[DataError]:
        """deduplicate ensure only the first instance of an error with the same
        message (as per calling ``str`` on the exception) is preserved

        >>> MultipleDataErrors.deduplicate([DataError("foo"), DataError("bar"), DataError("bar"),
        ...                                 DataError("foo"), DataError("baz")])
        [DataError('foo'), DataError('bar'), DataError('baz')]
        """
        seen: set[str] = set()
        deduplicated: list[DataError] = []
        for err in errors:
            desc = str(err)
            if desc not in seen:
                deduplicated.append(err)
                seen.add(desc)
        return deduplicated
