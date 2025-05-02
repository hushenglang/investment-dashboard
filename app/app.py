from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config.logging_config import setup_logging
import logging
from service.macro_data_service import MacroDataService
from controller.indicator_controller import router as indicator_router
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging(logging.DEBUG)
    yield

# Initialize FastAPI app
app = FastAPI(
    title="Investment Dashboard API",
    description="API for investment dashboard providing economic indicators and market data",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/swagger",    # Change Swagger UI path from /docs to /swagger
    redoc_url="/swagger-doc"      # Keep ReDoc at its default path
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(indicator_router)

# Initialize services
macro_service = MacroDataService()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
