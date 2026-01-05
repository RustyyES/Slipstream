import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_get_live_traffic(client: AsyncClient):
    response = await client.get("/traffic/live")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    # If simulated data exists, check structure
    if len(data) > 0:
        vehicle = data[0]
        assert "vehicle_id" in vehicle
        assert "latitude" in vehicle
        assert "longitude" in vehicle
        assert "speed" in vehicle
