import os


class Settings:
    CHROME_SLEEP: float = float(os.getenv("CHROME_SLEEP", "1"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "5"))


settings = Settings()

