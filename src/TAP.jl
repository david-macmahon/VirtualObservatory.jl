struct TAPService
    baseurl::URI
    format::Union{String,Nothing}
end
TAPService(baseurl::AbstractString; format=nothing) = TAPService(URI(baseurl), format)

_TAP_SERVICES = Dict(
    :vizier => TAPService("http://tapvizier.cds.unistra.fr/TAPVizieR/tap"),
    :simbad => TAPService("https://simbad.u-strasbg.fr/simbad/sim-tap"),
    :ned => TAPService("https://ned.ipac.caltech.edu/tap"),
    :gaia => TAPService("https://gea.esac.esa.int/tap-server/tap"),
    :cadc => TAPService("https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus"),
)
TAPService(service::Symbol) = _TAP_SERVICES[service]

@doc """
    TAPService(baseurl, [format="VOTABLE/TD"])
    TAPService(service::Symbol)

Handler of a service following the Virtual Observatory Table Access Protocol (TAP), as [defined](https://www.ivoa.net/documents/TAP/) by IVOA.
Instances of `TAPService` can be created by passing either a base URL of the service or a symbol corresponding to a known service:
$(@p keys(_TAP_SERVICES) |> collect |> sort |> map("`:$_`") |> join(__, ", ")).

A `TAPService` aims to follow the `DBInterface` interface, with query execution as the main feature: `execute(tap, query::String)`.
""" TAPService

connect(::Type{TAPService}, args...) = TAPService(args...)

struct TAPTable
    service::TAPService
    tablename::String
    unitful::Bool
    ra_col::String
    dec_col::String
    cols
end
TAPTable(service, tablename, cols=All(); unitful=true, ra_col="ra", dec_col="dec") = TAPTable(service, tablename, unitful, ra_col, dec_col, cols)

StructArrays.StructArray(t::TAPTable) = execute(StructArray, t.service, "select * from \"$(t.tablename)\"")

"""    execute([restype=StructArray], tap::TAPService, query::AbstractString; kwargs...)

Execute the ADQL `query` at the specified TAP service, and return the result as a `StructArray` - a fully featured Julia table.

`kwargs` are passed to `VOTables.read`, for example specify `unitful=true` to parse columns with units into `Unitful.jl` values.
"""
execute(tap::TAPService, query::AbstractString; upload=nothing, kwargs...) = execute(StructArray, tap, query; upload, kwargs...)
execute(T::Type, tap::TAPService, query::AbstractString; upload=nothing, kwargs...) = @p download(tap, query; upload) |> VOTables.read(T; kwargs...)

function Base.download(tap::TAPService, adqlquery::AbstractString, path=tempname(); upload=nothing)
    # URL and headers are the same regardless of upload
    syncurl = @p tap.baseurl |> @modify(joinpath(_, "sync"), __.path)
    headers = []

    # Method, body, query are different for uploading vs not uploading
    if isnothing(upload)
        # Not uploading
        method = "GET"
        body = []
        query = Pair{AbstractString,AbstractString}[
            "request" => "doQuery",
            "lang" => "ADQL",
            "query" => strip(adqlquery),
        ]
        isnothing(tap.format) || push!(query, "FORMAT" => tap.format);
    else
        # Uploading
        method = "POST"
        query = []
        body = begin
            formdata = Pair{String,Any}[
                "REQUEST" => "doQuery",
                "LANG" => "ADQL",
                "QUERY" => strip(adqlquery),
            ]
            isnothing(tap.format) || push!(formdata, "FORMAT" => tap.format)

            for (k,tbl) in pairs(upload)
                push!(formdata, "UPLOAD" => "$k,param:$k")

                vot_file = tempname()
                VOTables.write(vot_file, tbl)
                votiob = open(io->IOBuffer(read(io)), vot_file)
                push!(formdata, string(k) => HTTP.Multipart(basename(vot_file), votiob, "application/x-votable+xml"))
            end

            HTTP.Form(formdata)
        end
    end

    # Now make request and write response body to path
    open(path, "w") do response_stream
        # 1. Use require_ssl_verification=false to match the previous use of
        #    curl's --insecure option (even though tests pass without it).
        #
        # 2. Use pool=HTTP.Pool(1) to make tests pass (avoids concurrency
        #    issues?)
        #
        # The user can override these and/or use other HTTP.request kwargs by
        # putting them in the HTTP_REQUEST_OPTIONS Dict{Symbol,Any}.
        resp = HTTP.request(method, syncurl, headers, body;
            require_ssl_verification=false, # Allow user to override these...
            pool=HTTP.Pool(1),
            HTTP_REQUEST_OPTIONS..., # ...with kwargs given here...
            query, response_stream # ...but not these
        )

        if resp.status != HTTP.StatusCodes.OK
            @warn "Unexpected HTTP status code $(resp.status):\n$(resp.body)"
        end
    end

    return path
end
