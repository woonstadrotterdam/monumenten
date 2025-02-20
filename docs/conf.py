# Configuration file for the Sphinx documentation builder.

# -- Project information

project = "Monumenten"
copyright = "2025, Woonstad Rotterdam"
author = "Tomer Gabay, Ben Verhees, Tiddo Loos"

try:
    from monumenten._version import version as release

    version = ".".join(release.split(".")[:2])
except ImportError:
    try:
        from setuptools_scm import get_version

        release = get_version(root="..", relative_to=__file__)
        version = ".".join(release.split(".")[:2])
    except Exception:
        release = "0.1"
        version = "0.1.0"

# -- General configuration

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "myst_parser",
    "sphinx_design",
]

suppress_warnings = [
    "myst.strikethrough",  # to enble strikethrough, see: https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#strikethrough
]

myst_enable_extensions = ["strikethrough", "amsmath", "colon_fence"]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
}
intersphinx_disabled_domains = ["std"]

source_suffix = [".rst", ".md"]

templates_path = ["_templates"]

# -- Options for HTML output

html_theme = "sphinx_rtd_theme"

# -- Options for EPUB output
epub_show_urls = "footnote"
