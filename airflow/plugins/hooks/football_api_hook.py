import os, requests
from datetime import datetime, timezone

class FootballApiHook:
    def __init__(self):
        self.base = os.getenv("FOOTBALL_API_BASE_URL", "https://v3.football.api-sports.io").rstrip("/")
        self.key  = os.getenv("FOOTBALL_API_KEY", "")
        self.timeout = int(os.getenv("FOOTBALL_API_TIMEOUT", "15"))

    def _headers(self):
        return {
            "x-apisports-key": self.key,
            "Accept": "application/json",
            "User-Agent": "sporty-pipeline/0.1",
        }
    
    def get_fixtures(self, league: int, season: int, status: str):
        """
        Fetch fixtures for a given league+season.
        Default status='FT' returns only finished matches for labels/training.
        """
        url = f"{self.base}/fixtures"
        params = {"league": league, "season": season, "status": status}
        r = requests.get(url, headers=self._headers(), params=params, timeout=self.timeout)
        if r.status_code != 200:
            raise RuntimeError(f"API {r.status_code}: {r.text[:300]}")
        return {
            "endpoint": "fixtures",
            "params": params,
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "data": r.json(),
        }