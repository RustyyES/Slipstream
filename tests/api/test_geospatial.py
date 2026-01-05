import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_get_nearby_vehicles(client: AsyncClient):
    # Test London coordinates (where we know traffic exists)
    london_lat = 51.5074
    london_lon = -0.1278
    
    response = await client.get(f"/geospatial/nearby?lat={london_lat}&lon={london_lon}&radius_km=10")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    
    # If vehicles found, check distance logic
    if len(data) > 0:
        vehicle = data[0]
        assert "distance_meters" in vehicle
        # Distance should be <= 10km (10000m)
        assert vehicle["distance_meters"] <= 10000

@pytest.mark.asyncio
async def test_get_nearby_empty_ocean(client: AsyncClient):
    # Test coordinates in middle of Atlantic Ocean
    ocean_lat = 0.0
    ocean_lon = 0.0
    
    response = await client.get(f"/geospatial/nearby?lat={ocean_lat}&lon={ocean_lon}&radius_km=10")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 0 # Should count 0 vehicles
