import os

import redis
from celery.app import Celery

REDIS_URL = os.getenv("REDIS_URL") or "redis://localhost:6379/0"
celery = Celery(broker=REDIS_URL, backend=REDIS_URL, include=["src.kustoingest"])
redis_client = redis.Redis.from_url(REDIS_URL)
