import unittest
import json
import urllib.request
import urllib.parse
from datetime import datetime

API_BASE = "http://localhost:8000"

class TestUrbanAPI(unittest.TestCase):
    
    def get_json(self, endpoint, params=None):
        url = f"{API_BASE}{endpoint}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        
        try:
            with urllib.request.urlopen(url) as response:
                self.assertEqual(response.status, 200)
                data = response.read()
                return json.loads(data)
        except urllib.error.URLError as e:
            self.fail(f"API Request failed: {e}")

    def test_01_health_check(self):
        """Verify API is reachable."""
        data = self.get_json("/")
        self.assertIn("message", data)
        print("✅ Health Check Passed")

    def test_02_weather_summary(self):
        """Verify Weather endpoint returns data."""
        data = self.get_json("/weather/summary")
        self.assertIsInstance(data, list)
        if data:
            self.assertIn("avg_temp", data[0])
            self.assertIn("city", data[0])
        print(f"✅ Weather Endpoint Passed ({len(data)} cities)")

    def test_03_traffic_live(self):
        """Verify Live Traffic endpoint."""
        data = self.get_json("/traffic/live")
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0, "No traffic data found (Simulator might be stopped)")
        
        vehicle = data[0]
        self.assertIn("vehicle_id", vehicle)
        self.assertIn("speed", vehicle)
        print(f"✅ Traffic Endpoint Passed ({len(data)} vehicles)")

    def test_04_geospatial_search(self):
        """Verify Geospatial Radius Search."""
        # Search near London
        params = {
            "lat": 51.5074,
            "lon": -0.1278,
            "radius_km": 10
        }
        data = self.get_json("/geospatial/nearby", params)
        self.assertIsInstance(data, list)
        
        if data:
            v = data[0]
            self.assertIn("distance_meters", v)
            self.assertLessEqual(v["distance_meters"], 10000)
            print(f"✅ Geospatial Search Passed (Found {len(data)} vehicles near London)")
        else:
            print("⚠️ Geospatial Search returned 0 results (Check simulator distribution)")

if __name__ == '__main__':
    unittest.main(verbosity=2)
