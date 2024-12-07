from fr24.core import FR24, LiveFeedArrow
from datetime import datetime, timedelta
import asyncio
import logging
import os
import time
import httpx
from pathlib import Path
from typing import List
from random import random

LOGGING_FORMAT = "%(asctime)s [%(threadName)-12.12s] [%(filename)s:%(lineno)s] [%(levelname)s] %(message)s"

logging.basicConfig(
    level=logging.DEBUG if os.environ.get("DEBUG", None) else logging.INFO,
    format=LOGGING_FORMAT,
    handlers=[
        logging.StreamHandler()
    ]
)

async def log_response(response: httpx.Response):
    logging.info(f"FR24 response: {response.status_code} message: {response.headers.get('grpc-message', 'null')} {response.headers} {response}")

async def fetch_flights(
        from_date: datetime,
        to_date: datetime,
        step: timedelta,
        sleep: float = 5,
        jitter: float = 2,
        base_cache_dir: Path = Path('./cache')
        ) -> List[LiveFeedArrow]:
    data_list = []

    async with FR24() as fr24:
        fr24.http.client.event_hooks["response"].append(log_response)

        await fr24.login({"username": os.environ["USER"], "password": os.environ["PASS"]})
        dt = from_date

        while dt < to_date:
            resp = await fr24.live_feed.fetch(timestamp=dt)
            data_new = resp.to_arrow()
            data_new.save()
            data_new.save(fp=base_cache_dir / f"flights_{dt.strftime('%Y-%m-%dT%H-%M-%SZ')}.parquet")
            
            data_list.append(data_new)
            
            logging.info(f"Fetched data from {dt.isoformat()}")
            logging.debug(data_new.df)
            
            dt += step

            time.sleep(sleep + jitter * (2 * random() - 1))

asyncio.run(fetch_flights(
    from_date=datetime.fromisoformat("2023-12-31T09:00:00Z"),
    to_date=datetime.fromisoformat("2024-01-01T13:00:00Z"),
    step=timedelta(hours=1),
    sleep=30,
    jitter=10,
))