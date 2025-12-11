import csv, time, os, requests, dlt, datetime
from dotenv import load_dotenv
import logging
load_dotenv()

TOMTOM_KEY = os.environ["TOMTOM_API_KEY"]
CSV_FILE_50M = "/home/linh/project/vibe_code/Data/latlon_lines50m_BT.csv"
CSV_FILE_NOSPLIT = "/home/linh/project/vibe_code/Data/reverseGeocode_Point.csv"

logging.basicConfig(level=logging.INFO)

@dlt.resource(name = "TomTom_Traffic", primary_key = "osm_id")
def tomtom_traffic():
    with open(CSV_FILE_50M, encoding="utf-8") as f:
        for i, r in enumerate(csv.DictReader(f)):

            lat, lon = r["lat"], r["long"]

            url = (
              "https://api.tomtom.com/traffic/services/4/"
              "flowSegmentData/absolute/10/json"
            )

            data = requests.get(
                url,
                params={"point": f"{lat},{lon}", "unit": "KMPH", "key": TOMTOM_KEY}
            ).json()

            fs = data.get("flowSegmentData", {})

            yield {
                "osm_id": r["osm_id"],
                "lat": float(lat),              
                "lon": float(lon),
                "current_speed": fs.get("currentSpeed"),
                "free_flow_speed": fs.get("freeFlowSpeed"),
                "confidence": fs.get("confidence"),
                "road_closure": fs.get("roadClosure"),
                # event timestamp in UTC ISO format
                "observed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "raw": data
            }

            time.sleep(1)  

@dlt.resource(name="TomTom_reverseGeocode", primary_key="osm_id")
def tomtom_reverse_geocode():
    with open(CSV_FILE_NOSPLIT, encoding="utf-8") as f:
        for i, r in enumerate(csv.DictReader(f)):

            lat, lon = r["lat"], r["long"]

            url = f"https://api.tomtom.com/search/2/reverseGeocode/{lat},{lon}.json"

            data = requests.get(
                url,
                params={
                    "key": TOMTOM_KEY,
                    "radius": 100
                },
                timeout=10
            ).json()

            addr = {}
            pos_str = None
            if data.get("addresses"):
                addr_obj = data["addresses"][0]
                addr = addr_obj.get("address", {})
                pos_str = addr_obj.get("position")

            matched_lat = None
            matched_lon = None
            if pos_str and "," in pos_str:
                try:
                    matched_lat = float(pos_str.split(",")[0])
                    matched_lon = float(pos_str.split(",")[1])
                except Exception:
                    pass

            yield {
                "osm_id": r["osm_id"],
                "input_lat": float(lat),
                "input_lon": float(lon),

                "building_number": addr.get("buildingNumber"),
                "street": addr.get("street"),
                "street_name": addr.get("streetName"),
                "street_and_number": addr.get("streetNameAndNumber"),
                "ward": addr.get("municipalitySecondarySubdivision"),
                "district": addr.get("municipalitySubdivision"),
                "city": addr.get("municipality"),
                "country": addr.get("country"),
                "country_code": addr.get("countryCode"),
                "postal_code": addr.get("postalCode"),
                "freeform_address": addr.get("freeformAddress"),

                "matched_lat": matched_lat,
                "matched_lon": matched_lon,

                # event timestamp in UTC ISO format
                "observed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "raw": data
            }

            time.sleep(1)

@dlt.resource(name="OpenWeather_current", primary_key="osm_id")
def openweather_current():
    with open(CSV_FILE_NOSPLIT, encoding="utf-8") as f:
        for i, r in enumerate(csv.DictReader(f)):

            lat, lon = r["lat"], r["long"]

            url = "https://api.openweathermap.org/data/2.5/weather"

            data = requests.get(
                url,
                params={
                    "lat": lat,
                    "lon": lon,
                    "appid": os.environ["OPENWEATHER_API_KEY"],
                    "units": "metric"
                },
                timeout=10
            ).json()

            weather = data.get("weather", [{}])[0]
            main = data.get("main", {})
            wind = data.get("wind", {})
            clouds = data.get("clouds", {})
            sys = data.get("sys", {})

            yield {
                "osm_id": r["osm_id"],
                "lat": float(lat),
                "lon": float(lon),

                "weather_id": weather.get("id"),
                "weather_main": weather.get("main"),
                "weather_description": weather.get("description"),
                "weather_icon": weather.get("icon"),

                "temp": main.get("temp"),
                "feels_like": main.get("feels_like"),
                "temp_min": main.get("temp_min"),
                "temp_max": main.get("temp_max"),
                "humidity": main.get("humidity"),
                "pressure": main.get("pressure"),

                "wind_speed": wind.get("speed"),
                "wind_deg": wind.get("deg"),

                "clouds_all": clouds.get("all"),
                "visibility": data.get("visibility"),

                "sunrise": sys.get("sunrise"),
                "sunset": sys.get("sunset"),

                "timezone": data.get("timezone"),
                "city_id": data.get("id"),
                "city_name": data.get("name"),
                "country": sys.get("country"),

                # event timestamp in UTC ISO format
                "observed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "raw": data
            }

            time.sleep(1)


pipeline = dlt.pipeline(
    pipeline_name="tomtom",
    destination=dlt.destinations.postgres("postgresql://postgres:linh12345@localhost:15432/we_traf"),
    dataset_name="api_traffic_weather",
    )

if __name__ == "__main__":
    pipeline.run(tomtom_traffic())
    pipeline.run(tomtom_reverse_geocode())
    pipeline.run(openweather_current())
# logging.info(f"Pipeline finished with status: {success}")
