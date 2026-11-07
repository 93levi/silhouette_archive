import os
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from databricks import sql
from databricks.sdk.core import Config

cfg = Config()
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
S = "workspace.silhouette_archive"
SEASON_ORDER = "CASE season WHEN 'Resort' THEN 1 WHEN 'Spring' THEN 2 WHEN 'Pre-Fall' THEN 3 WHEN 'Fall' THEN 4 ELSE 5 END"

app = FastAPI(title="Silhouette Archive API")


def run(query, params=None):
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or {})
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def toggles(gender=None, line=None, shot_type=None, prefix=""):
    where, params = [], {}
    for name, value in [("gender", gender), ("line", line), ("shot_type", shot_type)]:
        if value:
            where.append(f"{prefix}{name} = :{name}")
            params[name] = value
    return where, params


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/houses")
def houses():
    return run(f"""
        SELECT designer, COUNT(*) AS collections
        FROM {S}.gold_house_by_season
        GROUP BY designer
        ORDER BY collections DESC
    """)


@app.get("/api/tags")
def tags():
    return run(f"""
        SELECT tag, SUM(tag_images) AS images
        FROM {S}.gold_tag_by_year
        GROUP BY tag
        ORDER BY images DESC
    """)


@app.get("/api/stack/house")
def house_stack(designer: str, gender: str = None, line: str = None):
    where, params = toggles(gender, line)
    where.insert(0, "designer = :designer")
    params["designer"] = designer
    rows = run(f"""
        SELECT year, season, gender, line, looks, hero_row_id, hero_url
        FROM {S}.gold_house_by_season
        WHERE {" AND ".join(where)}
        ORDER BY year, {SEASON_ORDER}
    """, params)
    if not rows:
        raise HTTPException(404, "No collections found")
    return rows


@app.get("/api/stack/tag")
def tag_stack(tag: str, gender: str = None, line: str = None, shot_type: str = None):
    where, params = toggles(gender, line, shot_type)
    extra = (" AND " + " AND ".join(where)) if where else ""
    params["tag"] = tag

    shares = run(f"""
        WITH t AS (
            SELECT year, SUM(tag_images) AS tag_images
            FROM {S}.gold_tag_by_year
            WHERE tag = :tag {extra}
            GROUP BY year
        ),
        tot AS (
            SELECT year, SUM(total_images) AS total_images
            FROM {S}.gold_totals_by_year
            WHERE 1 = 1 {extra}
            GROUP BY year
        )
        SELECT tot.year,
               COALESCE(t.tag_images, 0) AS tag_images,
               tot.total_images,
               COALESCE(t.tag_images, 0) / tot.total_images AS share
        FROM tot LEFT JOIN t ON tot.year = t.year
        ORDER BY tot.year
    """, params)

    ewhere, eparams = toggles(gender, line, shot_type, prefix="e.")
    eextra = (" AND " + " AND ".join(ewhere)) if ewhere else ""
    eparams["tag"] = tag
    heroes = run(f"""
        SELECT year, row_id, url, designer, season FROM (
            SELECT e.year, e.row_id, e.url, e.designer, e.season,
                   ROW_NUMBER() OVER (
                       PARTITION BY e.year
                       ORDER BY e.is_low_res, hash(e.row_id)
                   ) AS rn
            FROM {S}.silver_tags s
            JOIN {S}.silver_runway_enriched e ON s.row_id = e.row_id
            WHERE s.tag = :tag AND e.year BETWEEN 1990 AND 2020 {eextra}
        ) WHERE rn = 1
    """, eparams)

    by_year = {h["year"]: h for h in heroes}
    for r in shares:
        r["hero"] = by_year.get(r["year"])
    return shares


@app.get("/api/tag/year")
def tag_year(tag: str, year: int, gender: str = None, line: str = None, shot_type: str = None):
    where, params = toggles(gender, line, shot_type, prefix="e.")
    extra = (" AND " + " AND ".join(where)) if where else ""
    params.update({"tag": tag, "year": year})
    return run(f"""
        SELECT e.row_id, e.url, e.designer, e.season, e.shot_type, e.gender, e.line
        FROM {S}.silver_tags s
        JOIN {S}.silver_runway_enriched e ON s.row_id = e.row_id
        WHERE s.tag = :tag AND e.year = :year {extra}
        ORDER BY e.is_low_res, e.designer, e.row_id
    """, params)


@app.get("/api/collection")
def collection(designer: str, year: int, season: str):
    return run(f"""
        SELECT row_id, url, shot_type, gender, line
        FROM {S}.silver_runway_enriched
        WHERE designer = :designer AND year = :year AND season = :season
        ORDER BY row_id
    """, {"designer": designer, "year": year, "season": season})


@app.get("/api/look/tags")
def look_tags(row_id: int):
    return run(f"""
        SELECT tag FROM {S}.silver_tags
        WHERE row_id = :row_id
        ORDER BY tag
    """, {"row_id": row_id})


if os.path.isdir("static"):
    app.mount("/", StaticFiles(directory="static", html=True), name="static")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("DATABRICKS_APP_PORT", "8000")))