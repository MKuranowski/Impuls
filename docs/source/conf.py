# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Impuls"
copyright = "2024, Mikołaj Kuranowski"
author = "Mikołaj Kuranowski"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["sphinx.ext.autodoc", "sphinx.ext.autosummary"]
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "special-members": "__iter__,__next__,__enter__,__exit__,__contains__",
    "show-inheritance": True,
}
autodoc_mock_imports = ["impuls.extern.libextern"]

templates_path = ["_templates"]
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_static_path = ["_static"]