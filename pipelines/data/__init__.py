"""Static data files shipped alongside the pipelines package.

`__init__.py` exists so `importlib.resources.files("pipelines.data")` can
locate `watchlist.json` regardless of whether the package is run from the
source tree or installed into site-packages.
"""
