using TestItems
using TestItemRunner
@run_package_tests


@testitem "vizier catalog" begin
    using Dates
    using Unitful
    using VirtualObservatory: StructArray
    using VirtualObservatory.VOTables: DictArray

    c = table(VizierCatalog("J/ApJ/923/67/table2"))
    @test c.recno == 1:7728
    @test c[1].ID == "0003+380"
    @test c.var"nu-obs"[1] == 15.37f0
    @test c[2].Epoch == Date(2006, 12, 1)
    @test metadata(c.ID).description == "Source name in truncated B1950.0 coordinates"
    @test colmetadata(c, :ID) == metadata(c.ID)

    c = table(VizierCatalog("J/ApJ/923/67/table2", Cols([:ID, :Epoch])))
    @test c isa DictArray
    @test length(c[1]) == 2
    @test c[1].ID == "0003+380"

    c = table(VizierCatalog("J/ApJ/923/67/table2", Cols(:ID, :Epoch)))
    @test c isa StructArray
    @test c[1] === (ID = "0003+380", Epoch = Dates.Date("2006-03-09"))

    c = table(VizierCatalog("J/ApJ/923/67/table2"; unitful=true))
    @test c.recno == 1:7728
    @test c[1].ID == "0003+380"
    @test c.var"nu-obs"[1] == 15.37f0u"GHz"
    @test c[2].Epoch == Date(2006, 12, 1)
end

@testitem "TAP vizier" begin
    using Unitful

    @test TAPService("http://tapvizier.cds.unistra.fr/TAPVizieR/tap") == TAPService(:vizier)

    tbl = execute(TAPService(:vizier), """ select top 5 * from "II/246/out" """)
    @test length(tbl) == 5
    @test tbl[1].RAJ2000 == 44.996055

    tbl = execute(TAPService(:vizier), """ select top 5 * from "II/246/out" """; unitful=true)
    @test length(tbl) == 5
    @test tbl[1].RAJ2000 == 44.996055u"°"
end

@testitem "TAP vizier upload" begin
    using Unitful

    catalog = filter(r -> r.ID == "0003+380", table(VizierCatalog("J/ApJ/923/67/table2"); unitful=true))
    tbl = execute(TAPService(:vizier),
        """ select * from "J/ApJ/923/67/table2" inner join ids on "J/ApJ/923/67/table2".ID = TAP_UPLOAD.ids.id order by recno """;
        upload=(ids=(id=["0003+380"],),),
        unitful=true
    )
    @test length(tbl) == 10
    @test propertynames(tbl) == [propertynames(catalog); :id]
    @test tbl.ID == catalog.ID
    @test tbl.recno == catalog.recno
    @test tbl.Tb ≈ catalog.Tb

    tbl = execute(TAPService(:vizier),
        """ 
            select * from "J/ApJ/923/67/table2" inner join t1 on "J/ApJ/923/67/table2".recno = t1.no1 inner join t2 on "J/ApJ/923/67/table2".recno = t2.no2
        """;
        upload=(t1=(no1=[1, 2],), t2=(no2=[2, 3],)),
        unitful=true
    )
    @test tbl.recno == [2]
end

@testitem "TAP simbad" begin
    using Unitful

    tbl = execute(TAPService(:simbad), """select top 5 * from basic"""; unitful=true)
    @test length(tbl) == 5
    @test tbl.ra[1] isa typeof(1.0u"°")
end

@testitem "TAP ned" begin
    using Unitful

    tbl = execute(TAPService(:ned), """select top 5 * from objdir"""; unitful=true)
    @test length(tbl) == 5
    @test tbl[1].dec isa typeof(1.0u"°")
end

@testitem "TAP Gaia" begin
    tbl = execute(TAPService("https://gea.esac.esa.int/tap-server/tap", "VOTABLE_PLAIN"), "select top 5 * from gaiadr3.gaia_source order by source_id")
    @test length(tbl) == 5
    @test tbl[1].source_id == 4295806720
    @test tbl[1].designation == "Gaia DR3 4295806720"
end

@testitem "vizier xmatch" begin
    using FlexiJoins
    using SkyCoords
    using Unitful

    tbl = [
        (name="Abc", coords=ICRSCoords(0, 0)),
        (name="Def", coords=ICRSCoords(0.5, -0.1)),
    ]

    c = VizierCatalog("I/355/gaiadr3")
    J = innerjoin((; c, tbl), by_distance(identity, :coords, separation, <=(deg2rad(1/60))))
    @test length(J) == 5
    @test J[1].c.DR3Name == "Gaia DR3 2546034966433885568"
    @test J[1].c.RAdeg === 0.00943691398
    @test J.tbl[1] === tbl[1]

    # Ju = innerjoin((; c, tbl), by_distance(identity, :coords, separation, <=((1/60)u"°")))
    # @test Ju == J

    c = VizierCatalog("I/355/gaiadr3", Cols(:DR3Name, :RAdeg); unitful=true)
    J = innerjoin((; c, tbl), by_distance(identity, :coords, separation, <=(deg2rad(1/60))))
    @test length(J) == 5
    @test J[1].c === (DR3Name = "Gaia DR3 2546034966433885568", RAdeg = 0.00943691398u"°")
    @test J.tbl[1] === tbl[1]
end


@testitem "_" begin
    import Aqua
    Aqua.test_all(VirtualObservatory; ambiguities=false, piracy=false)
    Aqua.test_ambiguities(VirtualObservatory)

    import CompatHelperLocal as CHL
    CHL.@check()
end
