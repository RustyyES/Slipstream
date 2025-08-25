from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from src.api.database import database
from src.api.routers import traffic, weather, geospatial

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect to DB on startup
    await database.connect()
    print("Database connected.")
    yield
    # Disconnect on shutdown
    await database.disconnect()
    print("Database disconnected.")

app = FastAPI(
    title="Urban Mobility Analytics API",
    description="Real-time and batch analytics for urban traffic and weather.",
    version="1.0.0",
    lifespan=lifespan
)

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For dev only
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(traffic.router)
app.include_router(weather.router)
app.include_router(geospatial.router)

@app.get("/")
async def root():
    return {"message": "Urban Analytics API is running", "docs": "/docs"}
