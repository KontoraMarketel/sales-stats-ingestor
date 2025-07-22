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
    cards_list = cards['data']

    yesterday = get_yesterday_moscow_from_utc(ts)

    async with aiohttp.ClientSession(headers=headers) as session:
        for batch in chunked(cards_list, batch_size):
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

            # retry loop
            while True:
                try:
                    async with session.post(GET_SALES_STATS, json=payload) as response:
                        if response.status == 429:
                            logging.warning(f"Rate limited (429). Retrying after 10 seconds... NM IDs: {nm_ids}")
                            retry_after = int(response.headers['X-Ratelimit-Retry'])
                            await asyncio.sleep(retry_after)
                            continue  # retry

                        data = await response.json()

                        if response.status != 200:
                            logging.error(
                                f"Failed fetch for NM IDs {nm_ids}, status: {response.status}, response: {data}")
                            response.raise_for_status()

                        result.extend(data["data"])
                        break

                except aiohttp.ClientError as e:
                    logging.error(f"Network error fetching data for NM IDs {nm_ids}: {e}")
                    raise

            await asyncio.sleep(20)

    return result
