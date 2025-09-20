from fastapi import APIRouter
from src.api.database import database

router = APIRouter(
    prefix="/weather",
    tags=["weather"]
)

@router.get("/summary")
async def get_weather_summary():
    """
    Get the latest weather summary.
    """
    query = """
    SELECT * FROM serving.weather_summary;
    """
    results = await database.fetch_all(query)
    return [dict(row) for row in results]
