import os


class Settings:
    CHROME_SLEEP: float = float(os.getenv("CHROME_SLEEP", "1"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "5"))
    SELENIUM_REMOTE_URL: str = os.getenv("SELENIUM_REMOTE_URL", "").strip()


settings = Settings()

