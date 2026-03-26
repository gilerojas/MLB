"""
Mallitalytics MLB Content Hub — FastAPI server
Run: uvicorn api.main:app --port 8000 --reload
"""
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api.routers import cards, leaderboards, queue, schedule

REPO_ROOT = Path(__file__).parent.parent
OUTPUTS_DIR = REPO_ROOT / "outputs"
OUTPUTS_DIR.mkdir(exist_ok=True)

app = FastAPI(
    title="Mallitalytics MLB Hub",
    description="Content queue API for MLB card generation and Twitter posting.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve generated card PNGs
app.mount("/static", StaticFiles(directory=str(OUTPUTS_DIR)), name="static")

app.include_router(cards.router)
app.include_router(queue.router)
app.include_router(schedule.router)
app.include_router(leaderboards.router)


@app.get("/health")
def health():
    return {"status": "ok"}
