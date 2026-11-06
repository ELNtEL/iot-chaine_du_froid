"""Submits agent.manifest.json to the Noyau in the format POST /agents/submit really expects.
Usage:  python submit_agent.py                      (Noyau at http://localhost:8000)
        python submit_agent.py http://192.168.1.20:8000
"""
import json
import sys

import httpx

noyau = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000").rstrip("/")
m = json.load(open("agent.manifest.json", encoding="utf-8"))

submission = {
    "slug": m["slug"],
    "name": m["name"],
    "version": m["version"],
    "description": m["description"],
    "filiere": m["filiere"],
    "category": m["category"],
    "developer_email": m["developer"]["email"],
    "team_name": m.get("team_name"),
    "base_url": m["endpoints"]["base_url"],
    "pricing_model": m.get("pricing_model", "flat_monthly"),
    "price": m.get("price", 0),
    "produces": m.get("produces", []),
    "consumes": m.get("consumes", []),
    "manifest": m,
}
r = httpx.post(f"{noyau}/agents/submit", json=submission, timeout=10)
print(r.status_code, r.text)