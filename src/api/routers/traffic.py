from fastapi import APIRouter
from src.api.database import database

router = APIRouter(
    prefix="/traffic",
    tags=["traffic"]
)

@router.get("/live")
async def get_live_traffic():
    """
    Get the latest known location for all vehicles.
    """
    query = """
    SELECT 
        vehicle_id, 
        latitude, 
        longitude, 
        speed, 
        heading, 
        route_id, 
        timestamp
    FROM serving.vehicle_locations
    WHERE timestamp >= NOW() - INTERVAL '5 minutes'
    ORDER BY timestamp DESC;
    """
    results = await database.fetch_all(query)
    return [dict(row) for row in results]

@router.get("/stats")
async def get_traffic_stats():
    """
    Get daily traffic statistics.
    """
    query = """
    SELECT * FROM analytics.daily_traffic_stats
    ORDER BY date DESC
    LIMIT 10;
    """
    results = await database.fetch_all(query)
    return [dict(row) for row in results]
