import json
import logging
import random
import time
from typing import Any, Optional

from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from urllib3.exceptions import NewConnectionError
from webdriver_manager.chrome import ChromeDriverManager

try:
    from fastapi_app.config import settings
    from fastapi_app.helpers.constants import RPAQueryStatus
except ModuleNotFoundError:
    from config import settings
    from helpers.constants import RPAQueryStatus


logger = logging.getLogger("money")


class MoneyCheckHelper:
    v2_url = "https://cdcb3.judicial.gov.tw/judbp/wkw/WHD9A01/V2.htm"
    query_titles = {1: "debt", 2: "bankrupt"}

    def __init__(self, idnum: str, name: str) -> None:
        self.idnum = idnum
        self.name = name
        self.query_result: dict[str, Any] = {}
        self.pdf_result: dict[str, Any] = {}

    def get_params(self, query_type: int) -> str:
        payload = {
            "queryType": str(query_type),
            "clnm": self.name,
            "idno": self.idnum,
            "sddt_s": "",
            "sddt_e": "",
            "crtid": "",
        }
        return json.dumps(payload)

    @staticmethod
    def get_random_sleep() -> float:
        return round(random.random(), 1)

    @staticmethod
    def _safe_quit(driver: Optional[webdriver.Chrome]) -> None:
        if not driver:
            return
        try:
            driver.close()
        except Exception:
            pass
        try:
            driver.quit()
        except Exception:
            pass

    def get_driver(self) -> webdriver.Chrome:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")
        debug_port = random.randint(9222, 9240)
        chrome_options.add_argument(f"--remote-debugging-port={debug_port}")
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=chrome_options)

    def get_js(self, params: str, action: str = "QUERY") -> str:
        return f"""
        var d = '{params}';
        var a = JSON.parse(d);
        var done = arguments[0];
        formUtil.bindFormData($queryForm, a);
        $("#pageNum", $queryForm).val(1);
        $("#pageSize", $queryForm).val(20);
        formUtil.submitTo({{
            url: "wkw/WHD9A01/{action}.htm",
            formObj: $queryForm,
            async: true,
            onSuccess: function(responseBean) {{
                done(responseBean)
            }}
        }});
        return done
        """

    def check(
        self, query_type: int
    ) -> tuple[RPAQueryStatus, Optional[int], Optional[str], Optional[dict[str, Any]]]:
        if query_type not in (1, 2):
            return RPAQueryStatus.ERROR, None, None, None

        for attempt in range(settings.MAX_RETRIES + 1):
            driver: Optional[webdriver.Chrome] = None
            try:
                time.sleep(self.get_random_sleep())
                driver = self.get_driver()
                driver.get(self.v2_url)

                params = self.get_params(query_type)
                query_result = driver.execute_async_script(self.get_js(params, "QUERY"))
                if query_result is None:
                    logger.warning("%s query_result is None, retrying", self.query_titles[query_type])
                    continue

                page_info = query_result.get("pageInfo", {})
                total_num = page_info.get("totalNum") if isinstance(page_info, dict) else None
                check_result = (
                    RPAQueryStatus.NORMAL if total_num == 0 else RPAQueryStatus.ABNORMAL
                )

                time.sleep(settings.CHROME_SLEEP)
                pdf_result = driver.execute_async_script(self.get_js(params, "PRINT"))
                pdf_url = pdf_result.get("data") if isinstance(pdf_result, dict) else None
                if not pdf_url:
                    logger.warning("%s pdf_url is empty, retrying", self.query_titles[query_type])
                    continue

                self.query_result = query_result
                self.pdf_result = pdf_result
                return check_result, total_num, pdf_url, query_result

            except (WebDriverException, NewConnectionError) as exc:
                logger.warning("%s webdriver error on attempt %s: %s", self.query_titles[query_type], attempt + 1, exc)
            except Exception as exc:
                logger.warning("%s unexpected error on attempt %s: %s", self.query_titles[query_type], attempt + 1, exc)
            finally:
                self._safe_quit(driver)

        return RPAQueryStatus.ERROR, None, None, None

    def check_debt(
        self,
    ) -> tuple[RPAQueryStatus, Optional[int], Optional[str], Optional[dict[str, Any]]]:
        return self.check(1)

    def check_bankrupt(
        self,
    ) -> tuple[RPAQueryStatus, Optional[int], Optional[str], Optional[dict[str, Any]]]:
        return self.check(2)

