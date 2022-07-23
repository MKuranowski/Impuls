import re


def camel_to_snake(camel: str) -> str:
    """Converts camelCase or PascalCase to snake_case.

    >>> camel_to_snake("Foo")
    'foo'
    >>> camel_to_snake("FooBar")
    'foo_bar'
    >>> camel_to_snake("fooBarBaz")
    'foo_bar_baz'
    """
    return re.sub(r"\B[A-Z]", lambda m: f"_{m[0]}", camel).lower()
