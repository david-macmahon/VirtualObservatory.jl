# VirtualObservatory.jl

Access remote data and services that follow Virtual Observatory (VO, https://www.ivoa.net/) protocols.

Currently, supports the Table Access Protocol (TAP), and also a couple of features specific to the VizieR database. \
See the docstrings for `TAPService` and `VizierCatalog` for more details.

`TAPService` queries now use `HTTP.request` from `HTTP.jl`.  Additional keyword
arguments for `HTTP.request` may be given in `VirtualObservator.HTTP_REQUEST_OPTIONS`.
See the docstrings for `HTTP_REQUEST_OPTIONS` and `HTTP.request` for more details.
