import logging
import httpx

logger = logging.getLogger(__name__)

FROST_THRESHOLD_C = 2
RAIN_PROB_THRESHOLD_PCT = 60
RAIN_MM_THRESHOLD = 10
FORECAST_ENTRY_COUNT = 16


async def fetch_current_weather(api_key: str, lat: str, lon: str) -> str:
    """Fetch current weather from OpenWeatherMap."""
    if not all([api_key, lat, lon]):
        return "Weather configuration missing."

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric"
    }

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            weather_desc = data.get("weather", [{}])[0].get("description", "unknown")
            temp = data.get("main", {}).get("temp", "unknown")
            return f"Current Weather: {weather_desc}, Temperature: {temp}°C"
        except Exception as e:
            logger.error(f"Failed to fetch weather: {e}")
            return "Could not fetch weather data."


async def fetch_forecast(api_key: str, lat: str, lon: str) -> dict:
    """Fetch 48-hour forecast from OpenWeatherMap (5-day/3-hour endpoint).

    Returns dict with:
      summary: human-readable forecast string
      frost_risk: bool (any temp <= FROST_THRESHOLD_C)
      rain_alert: bool (>= RAIN_PROB_THRESHOLD_PCT chance or >= RAIN_MM_THRESHOLD total)
      min_temp_c: float
      max_rain_mm: float
      max_rain_prob: float
    """
    if not all([api_key, lat, lon]):
        return {"summary": "Forecast configuration missing.", "frost_risk": False, "rain_alert": False}

    url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric",
        "cnt": FORECAST_ENTRY_COUNT,  # 16 x 3hr = 48 hours
    }

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            entries = data.get("list", [])
            if not entries:
                return {"summary": "No forecast data available.", "frost_risk": False, "rain_alert": False}

            min_temp = min(e.get("main", {}).get("temp_min", 99) for e in entries)
            max_temp = max(e.get("main", {}).get("temp_max", -99) for e in entries)
            max_rain_prob = max(e.get("pop", 0) for e in entries) * 100  # pop is 0-1
            total_precip = sum(
                e.get("rain", {}).get("3h", 0) + e.get("snow", {}).get("3h", 0)
                for e in entries
            )

            frost_risk = min_temp <= FROST_THRESHOLD_C
            rain_alert = max_rain_prob >= RAIN_PROB_THRESHOLD_PCT or total_precip >= RAIN_MM_THRESHOLD

            parts = [f"48-Hour Forecast: Low {min_temp:.0f}°C / High {max_temp:.0f}°C"]
            if max_rain_prob > 0:
                parts.append(f"Rain chance up to {max_rain_prob:.0f}%, total precip {total_precip:.1f}mm")
            if frost_risk:
                parts.append(f"⚠ FROST RISK — temps dropping to {min_temp:.0f}°C")
            if rain_alert:
                parts.append("🌧 Significant rain expected")

            return {
                "summary": ". ".join(parts),
                "frost_risk": frost_risk,
                "rain_alert": rain_alert,
                "min_temp_c": min_temp,
                "max_rain_mm": total_precip,
                "max_rain_prob": max_rain_prob,
            }
        except Exception as e:
            logger.error(f"Failed to fetch forecast: {e}")
            return {"summary": "Could not fetch forecast data.", "frost_risk": False, "rain_alert": False}
