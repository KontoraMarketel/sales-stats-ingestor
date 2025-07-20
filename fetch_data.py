import asyncio
import logging
import aiohttp

from utils import get_yesterday_moscow_from_utc, chunked

GET_SALES_STATS = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"


async def fetch_data(api_token: str, cards: list, ts) -> list:
    headers = {
        "Authorization": api_token
    }
    batch_size = 20
    result = []

    yesterday = get_yesterday_moscow_from_utc(ts)

    async with aiohttp.ClientSession(headers=headers) as session:
        for batch in chunked(cards, batch_size):
            nm_ids = [i['nmID'] for i in batch]
            logging.info("Fetching data from NM IDs: {}".format(nm_ids))
            payload = {
                "nmIDs": nm_ids,
                "period": {
                    "begin": yesterday,
                    "end": yesterday
                },
                "timezone": "Europe/Moscow",
                "aggregationLevel": "day"
            }
            async with session.post(GET_SALES_STATS, json=payload) as response:
                data = await response.json()
                if response.status != 200:
                    logging.error("Error fetching data from NM IDs: {} Error response", nm_ids, data)
                    response.raise_for_status()

                result.extend(data["data"])
            await asyncio.sleep(20)

    return result
