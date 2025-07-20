from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def chunked(data, size):
    for i in range(0, len(data), size):
        yield data[i:i + size]



def get_yesterday_moscow_from_utc(utc_time: str) -> str:
    # Преобразуем в datetime с учётом часового пояса UTC
    dt = datetime.fromisoformat(utc_time)
    # Переводим во временную зону Europe/Moscow
    dt_moscow = dt.astimezone(ZoneInfo("Europe/Moscow"))

    # Вычитаем 1 день
    yesterday_moscow = dt_moscow - timedelta(days=1)
    # Приводим к нужному формату
    formatted = yesterday_moscow.strftime("%Y-%m-%d")
    return formatted

