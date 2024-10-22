import shapely
from rtree import index


def create_spatial_index(beschermde_gebieden):
    beschermde_gezicht_geometrieën = [
        (gebied["naam"], shapely.io.from_wkt(gebied["gezichtWKT"]))
        for gebied in beschermde_gebieden
    ]
    idx = index.Index()
    for pos, (naam, geom) in enumerate(beschermde_gezicht_geometrieën):
        idx.insert(pos, shapely.bounds(geom))
    return idx, beschermde_gezicht_geometrieën


def controleer_punt(punt_id, punt_geom, idx, beschermde_gezicht_geometrieën):
    mogelijke_overeenkomsten = idx.intersection(shapely.bounds(punt_geom))
    for match in mogelijke_overeenkomsten:
        naam, geom = beschermde_gezicht_geometrieën[match]
        if shapely.contains(geom, punt_geom):
            return naam
    return None
