from fastapi import APIRouter, HTTPException, Query
from src.api.database import database

router = APIRouter(
    prefix="/geospatial",
    tags=["geospatial"]
)

@router.get("/nearby")
async def get_vehicles_nearby(
    lat: float = Query(..., description="Latitude of the center point"),
    lon: float = Query(..., description="Longitude of the center point"),
    radius_km: float = Query(5.0, description="Search radius in Kilometers")
):
    """
    Find all vehicles within a specific radius of a given point using PostGIS.
    """
    # Convert km to meters for ST_DWithin (if using geography) or stay in degrees if simple geometry.
    # Since we are using 4326 (lat/lon), ST_DWithin on GEOMETRY uses degrees, which is hard to calculate.
    # Better to cast to GEOGRAPHY for meter-based calculations.
    
    radius_meters = radius_km * 1000
    
    query = """
    SELECT 
        vehicle_id, 
        latitude, 
        longitude, 
        speed, 
        heading,
        ST_Distance(
            geom::geography, 
            ST_MakePoint(:lon, :lat)::geography
        ) as distance_meters
    FROM serving.vehicle_locations
    WHERE ST_DWithin(
        geom::geography, 
        ST_MakePoint(:lon, :lat)::geography, 
        :radius_meters
    )
    ORDER BY distance_meters ASC
    LIMIT 50;
    """
    
    try:
        results = await database.fetch_all(query, values={
            "lat": lat, 
            "lon": lon, 
            "radius_meters": radius_meters
        })
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
