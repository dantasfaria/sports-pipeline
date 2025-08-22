import os, requests
from datetime import datetime, timezone

class FootballApiHook:
    def __init__(self):
        # API-Sports direct URL
        self.base = os.getenv("FOOTBALL_API_BASE_URL", "https://v3.football.api-sports.io").rstrip("/")
        self.key  = os.getenv("FOOTBALL_API_KEY", "")
        self.timeout = int(os.getenv("FOOTBALL_API_TIMEOUT", "15"))

    def _headers(self):
        # API-Sports expects ONLY this header (no RapidAPI headers)
        return {
            "x-apisports-key": self.key,
            "Accept": "application/json",
            "User-Agent": "sporty-pipeline/0.1",
        }

    def get_fixtures_today(self, league_id=None):
        url = f"{self.base}/fixtures"
        params = {"date": datetime.now(timezone.utc).date().isoformat()}
        if league_id:
            params["league"] = str(league_id)
        r = requests.get(url, headers=self._headers(), params=params, timeout=self.timeout)
        # Raise a helpful error if auth fails
        if r.status_code != 200:
            raise RuntimeError(f"API {r.status_code}: {r.text[:300]}")
        # Return an envelope so downstream ops can log/trace
        return {
            "endpoint": "fixtures",
            "params": params,
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "data": r.json(),
        }
