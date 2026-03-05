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


logger = logging.getLogger("money")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class MoneyCheckHelper:
    base_url = "https://cdcb3.judicial.gov.tw/judbp"
    v2_url = f"{base_url}/wkw/WHD9A01/V2.htm"
    query_url = f"{base_url}/wkw/WHD9A01/QUERY.htm"
    print_url = f"{base_url}/wkw/WHD9A01/PRINT.htm"
    query_titles = {1: "debt", 2: "bankrupt"}

    def __init__(self, idnum: str, name: str) -> None:
        self.idnum = idnum
        self.name = name
        self.query_result: dict[str, Any] = {}
        self.pdf_result: dict[str, Any] = {}

    def get_form_data(self, query_type: int) -> dict[str, str]:
        return {
            "queryType": str(query_type),
            "clnm": self.name,
            "idno": self.idnum,
            "sddt_s": "",
            "sddt_e": "",
            "crtid": "",
            "pageNum": "1",
            "pageSize": "20",
        }

    @staticmethod
    def get_random_sleep() -> float:
        return round(random.random(), 1)

    def check(
        self, query_type: int
    ) -> tuple[RPAQueryStatus, Optional[int], Optional[str], Optional[dict[str, Any]]]:
        if query_type not in (1, 2):
            return RPAQueryStatus.ERROR, None, None, None

        for attempt in range(settings.MAX_RETRIES + 1):
            try:
                time.sleep(self.get_random_sleep())
                form_data = self.get_form_data(query_type)
                with httpx.Client(
                    follow_redirects=True,
                    timeout=30.0,
                    headers={"User-Agent": USER_AGENT},
                ) as client:
                    r = client.get(self.v2_url)
                    r.raise_for_status()

                    post_headers = {"Referer": self.v2_url, "Origin": self.base_url}
                    r = client.post(self.query_url, data=form_data, headers=post_headers)
                    r.raise_for_status()
                    query_result = r.json()
                    if not isinstance(query_result, dict):
                        logger.warning("%s query_result is not dict (got %s), retrying", self.query_titles[query_type], type(query_result).__name__)
                        continue

                    page_info = query_result.get("pageInfo") or {}
                    total_num = page_info.get("totalNum") if isinstance(page_info, dict) else None
                    check_result = (
                        RPAQueryStatus.NORMAL if total_num == 0 else RPAQueryStatus.ABNORMAL
                    )

                    time.sleep(settings.CHROME_SLEEP)
                    r = client.post(self.print_url, data=form_data, headers=post_headers)
                    r.raise_for_status()
                    pdf_result = r.json()
                    pdf_url = pdf_result.get("data") if isinstance(pdf_result, dict) else None
                    if not pdf_url:
                        logger.warning("%s pdf_url is empty (response: %s), retrying", self.query_titles[query_type], pdf_result)
                        continue

                    self.query_result = query_result
                    self.pdf_result = pdf_result
                    return check_result, total_num, pdf_url, query_result

            except httpx.HTTPError as exc:
                logger.warning("%s http error on attempt %s: %s", self.query_titles[query_type], attempt + 1, exc)
            except Exception as exc:
                logger.warning("%s unexpected error on attempt %s: %s", self.query_titles[query_type], attempt + 1, exc)

        return RPAQueryStatus.ERROR, None, None, None

    def check_debt(
        self,
    ) -> tuple[RPAQueryStatus, Optional[int], Optional[str], Optional[dict[str, Any]]]:
        return self.check(1)

    def check_bankrupt(
        self,
    ) -> tuple[RPAQueryStatus, Optional[int], Optional[str], Optional[dict[str, Any]]]:
        return self.check(2)
