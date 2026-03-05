# RPA

## Linux 伺服器設定（修正 chromedriver exit code 127）

在 Linux 上若出現 `chromedriver unexpectedly exited. Status code was: 127`，代表 **Chrome 未安裝** 或 **缺少執行 chromedriver 所需的系統函式庫**。請在伺服器上安裝 Chrome 與依賴。

### Debian / Ubuntu（apt）

```bash
# 安裝 Google Chrome 與依賴（推薦）
wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" | sudo tee /etc/apt/sources.list.d/google-chrome.list
sudo apt-get update
sudo apt-get install -y google-chrome-stable

# 若只裝 Chromium，也需安裝 chromedriver 依賴
sudo apt-get install -y \
  libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
  libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
  libgbm1 libasound2 libpango-1.0-0 libcairo2
```

### 使用 Docker 時

在 Dockerfile 中安裝 Chrome 與依賴，例如：

```dockerfile
RUN apt-get update && apt-get install -y \
  wget gnupg ca-certificates \
  && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg \
  && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
  && apt-get update \
  && apt-get install -y google-chrome-stable \
  && rm -rf /var/lib/apt/lists/*
```

### 驗證

安裝後可測試：

```bash
google-chrome-stable --version
# 或
chromium --version
```

本專案使用 `webdriver-manager` 自動下載對應版本的 chromedriver，只要系統有正確安裝 Chrome 與上述依賴即可正常執行。
