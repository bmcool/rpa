import logging
import random
import time
from typing import Any, Optional

import httpx

try:
    from fastapi_app.config import settings
    from fastapi_app.helpers.constants import RPAQueryStatus
except ModuleNotFoundError:
    from config import settings
    from helpers.constants import RPAQueryStatus


logger = logging.getLogger("domestic")

# 與瀏覽器一致，避免被擋
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class DomesticJudV2Helper:
    base_url = "https://domestic.judicial.gov.tw/judbp"
    v2_url = f"{base_url}/wkw/WHD9HN01/V2.htm"
    query_url = f"{base_url}/wkw/WHD9HN01/QUERY.htm"
    print_url = f"{base_url}/wkw/WHD9HN01/PRINT.htm"

    def __init__(self, idnum: str, name: str) -> None:
        self.idnum = idnum
        self.name = name
        self.query_result: dict[str, Any] = {}
        self.pdf_result: dict[str, Any] = {}

    @staticmethod
    def get_random_sleep() -> float:
        return round(random.random(), 1)

    def get_n_check_data(self) -> tuple[RPAQueryStatus, Optional[int], Optional[str], Optional[dict[str, Any]]]:
        for attempt in range(settings.MAX_RETRIES + 1):
            try:
                time.sleep(self.get_random_sleep())
                with httpx.Client(
                    follow_redirects=True,
                    timeout=30.0,
                    headers={"User-Agent": USER_AGENT},
                ) as client:
                    # 先 GET 取得 session / cookie
                    r = client.get(self.v2_url)
                    r.raise_for_status()

                    # POST 查詢（表單欄位與前端一致）
                    query_form = {
                        "clnm": self.name,
                        "idno": self.idnum,
                        "pageNum": "1",
                        "pageSize": "20",
                    }
                    r = client.post(self.query_url, data=query_form)
                    r.raise_for_status()
                    query_result = r.json()
                    if not isinstance(query_result, dict):
                        logger.warning("domestic query_result is not dict (got %s), retrying", type(query_result).__name__)
                        continue

                    page_info = query_result.get("pageInfo") or {}
                    total_num = page_info.get("totalNum") if isinstance(page_info, dict) else None
                    check_result = (
                        RPAQueryStatus.NORMAL if total_num == 0 else RPAQueryStatus.ABNORMAL
                    )

                    time.sleep(settings.CHROME_SLEEP)
                    r = client.post(self.print_url, data=query_form)
                    r.raise_for_status()
                    pdf_result = r.json()
                    pdf_url = pdf_result.get("data") if isinstance(pdf_result, dict) else None
                    if not pdf_url:
                        logger.warning("domestic pdf_url is empty (response: %s), retrying", pdf_result)
                        continue

                    self.query_result = query_result
                    self.pdf_result = pdf_result
                    return check_result, total_num, pdf_url, query_result

            except httpx.HTTPError as exc:
                logger.warning("domestic http error on attempt %s: %s", attempt + 1, exc)
            except Exception as exc:
                logger.warning("domestic unexpected error on attempt %s: %s", attempt + 1, exc)

        return RPAQueryStatus.ERROR, None, None, None
