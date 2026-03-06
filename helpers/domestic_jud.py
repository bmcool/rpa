import logging
import random
import time
from typing import Any, Optional

from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from urllib3.exceptions import NewConnectionError

from config import settings
from helpers.constants import RPAQueryStatus


logger = logging.getLogger("domestic")

QUERY_SCRIPT = """
var done = arguments[0];
allForm.pageNum.value = 1;
allForm.pageSize.value = 20;
querydata = null;
formUtil.submitTo({
    url: "wkw/WHD9HN01/QUERY.htm",
    formObj: $queryForm,
    async: true,
    onSuccess: function(responseBean) {
        done(responseBean)
    }
});
return done
"""

PRINT_SCRIPT = """
var done = arguments[0];
formUtil.submitTo({
    url: "wkw/WHD9HN01/PRINT.htm",
    formObj: $queryForm,
    async: true,
    onSuccess: function(responseBean) {
        done(responseBean)
    }
});
return done
"""


class DomesticJudV2Helper:
    v2_url = "https://domestic.judicial.gov.tw/judbp/wkw/WHD9HN01/V2.htm"

    def __init__(self, idnum: str, name: str) -> None:
        self.idnum = idnum
        self.name = name
        self.query_result: dict[str, Any] = {}
        self.pdf_result: dict[str, Any] = {}

    def get_driver(self) -> WebDriver:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        if settings.SELENIUM_REMOTE_URL:
            return webdriver.Remote(
                command_executor=settings.SELENIUM_REMOTE_URL,
                options=chrome_options,
            )

        debug_port = random.randint(9222, 9240)
        chrome_options.add_argument(f"--remote-debugging-port={debug_port}")
        return webdriver.Chrome(options=chrome_options)

    @staticmethod
    def _safe_quit(driver: Optional[WebDriver]) -> None:
        if not driver:
            return
        try:
            driver.delete_all_cookies()
            driver.quit()
        except Exception:
            pass

    @staticmethod
    def get_random_sleep() -> float:
        return round(random.random(), 1)

    def get_n_check_data(self) -> tuple[RPAQueryStatus, Optional[int], Optional[str], Optional[dict[str, Any]]]:
        for attempt in range(settings.MAX_RETRIES + 1):
            driver: Optional[WebDriver] = None
            try:
                time.sleep(self.get_random_sleep())
                driver = self.get_driver()
                driver.get(self.v2_url)
                driver.set_script_timeout(10)

                set_value_script = """
                    var elem = arguments[0];
                    var value = arguments[1];
                    elem.value = value;
                """
                clnm_input = driver.find_element(By.ID, "clnm")
                driver.execute_script(set_value_script, clnm_input, self.name)
                idno_input = driver.find_element(By.ID, "idno")
                driver.execute_script(set_value_script, idno_input, self.idnum)

                driver.execute_script("$queryForm = $(\"#queryForm\");")
                query_result = driver.execute_async_script(QUERY_SCRIPT)
                if query_result is None:
                    logger.warning("domestic query_result is None, retrying")
                    continue

                total_num = query_result.get("pageInfo", {}).get("totalNum")
                check_result = (
                    RPAQueryStatus.NORMAL if total_num == 0 else RPAQueryStatus.ABNORMAL
                )

                time.sleep(settings.CHROME_SLEEP)
                pdf_result: dict[str, Any] = driver.execute_async_script(PRINT_SCRIPT)
                pdf_url = pdf_result.get("data") if isinstance(pdf_result, dict) else None
                if not pdf_url:
                    logger.warning("domestic pdf_url is empty, retrying")
                    continue

                self.query_result = query_result
                self.pdf_result = pdf_result
                return check_result, total_num, pdf_url, query_result

            except (WebDriverException, NewConnectionError) as exc:
                logger.warning("domestic webdriver error on attempt %s: %s", attempt + 1, exc)
            except Exception as exc:
                logger.warning("domestic unexpected error on attempt %s: %s", attempt + 1, exc)
            finally:
                self._safe_quit(driver)

        return RPAQueryStatus.ERROR, None, None, None

