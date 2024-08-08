# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

# pyright: basic
from pathlib import Path
from sys import exit

from mesonbuild import mparser

SOURCE_EXTENSIONS = ("c", "zig", "py")


def read_meson_build() -> mparser.CodeBlockNode:
    content = Path("meson.build").read_text("utf-8")
    return mparser.Parser(content, "meson.build").parse()


def find_declared_python_sources(meson_build: mparser.CodeBlockNode) -> set[str]:
    python_sources = set[str]()

    for node in meson_build.lines:
        # Ignore any nodes other than "py.install_sources(...)"
        if (
            not isinstance(node, mparser.MethodNode)
            or not isinstance(node.source_object, mparser.IdNode)
            or node.source_object.value != "py"
            or node.name.value != "install_sources"
        ):
            continue

        python_sources.update(
            arg.value for arg in node.args.arguments if isinstance(arg, mparser.StringNode)
        )

    return python_sources


def find_declared_zig_sources(meson_build: mparser.CodeBlockNode) -> set[str]:
    zig_sources = set[str]()

    for node in meson_build.lines:
        # Ignore anything other than "custom_target('libextern', ...)"
        if (
            not isinstance(node, mparser.FunctionNode)
            or node.func_name.value != "custom_target"
            or getattr(node.args.arguments[0], "value", "") != "libextern"
        ):
            continue

        # Extract input arguments from input keyword arguments
        for keyword, value in node.args.kwargs.items():
            if getattr(keyword, "value", "") != "input" or not isinstance(
                value, mparser.ArrayNode
            ):
                continue

            zig_sources.update(
                arg.value for arg in value.args.arguments if isinstance(arg, mparser.StringNode)
            )

    return zig_sources


def find_actual_sources() -> set[str]:
    sources = {"impuls/py.typed"}
    for ext in SOURCE_EXTENSIONS:
        sources.update(str(f) for f in Path("impuls").glob(f"**/*.{ext}"))
    return sources


def main() -> int:
    build = read_meson_build()
    defined_sources = find_declared_python_sources(build) | find_declared_zig_sources(build)
    actual_sources = find_actual_sources()

    ok = True
    unknown_defined_sources = defined_sources - actual_sources
    if unknown_defined_sources:
        ok = False
        print("✘ The following sources from meson.build couldn't be found:")
        for file in sorted(unknown_defined_sources):
            print(f"- {file}")

    undeclared_sources = actual_sources - defined_sources
    if undeclared_sources:
        ok = False
        print("✘ The following sources weren't declared in meson.build:")
        for file in sorted(undeclared_sources):
            print(f"- {file}")

    if ok:
        print("✔ Source files match with declarations in meson.build")
        return 0
    else:
        return 1


if __name__ == "__main__":
    exit(main())
