"""Allow running with: python -m app"""
from app.main import main
import asyncio

asyncio.run(main())
