import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_get_weather_summary(client: AsyncClient):
    response = await client.get("/weather/summary")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    
    if len(data) > 0:
        weather = data[0]
        assert "avg_temp" in weather
        assert "avg_humidity" in weather
        assert "city" in weather
