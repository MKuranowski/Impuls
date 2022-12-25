from typing import Iterable, TypeVar

_T = TypeVar("_T")


class InputNotModified(Exception):
    pass


class DataError(ValueError):
    """DataError represents any error related to incorrect input data.

    The main point of DataErrors is that it may be caught
    and any underlying process may continue. Thus, any process raising
    DataError must not leave the pipeline in an undefined state.
    """

    pass


class MultipleDataErrors(DataError):
    """MultipleDataErrors is raised when a process encounters a non-zero amount of DataErrors.

    For most use cases the catch_all helper can be used to catch any DataErrors
    that might be encountered.
    """

    def __init__(self, when: str, errors: list[DataError]) -> None:
        self.errors = errors
        super().__init__(
            f"{len(errors)} error(s) encountered during {when}:\n    "
            + "\n    ".join(err.args[0] for err in errors)
        )

    @classmethod
    def catch_all(cls, context: str, may_raise_data_error: Iterable[_T]) -> list[_T]:
        """catch_all takes a generator that may raise DataError when retrieving
        the next item; and catches all the DataErrors to raise a single
        MultipleDataErrors once the generator is exhausted.

        If no DataErrors were thrown, returns all non-None items
        from the generator.

        Any other Exception is passed through to the caller.

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
            raise MultipleDataErrors(context, errors)

        return elements
