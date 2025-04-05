# 1) Базовый образ c Python 3.10 (slim-версия)
FROM python:3.10-slim

# 2) Устанавливаем системные библиотеки, нужные для Chromium (Playwright)
RUN apt-get update && apt-get install -y \
    wget gnupg apt-transport-https \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libxcomposite1 libxdamage1 \
    libxfixes3 libnspr4 libxrandr2 libgbm1 libasound2 libpangocairo-1.0-0 \
    libatspi2.0-0 libgtk-3-0 libdrm2 libxshmfence1 \
    && rm -rf /var/lib/apt/lists/*

# 3) Создадим директорию /app и перейдём в неё
WORKDIR /app

# 4) Скопируем файл requirements.txt в контейнер и установим питон-пакеты
COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# 5) Устанавливаем движок Chromium для Playwright
#    (playwright install --with-deps chromium) иногда не нужно, 
#    но лучше явно прописать.
RUN pip3 install playwright
RUN playwright install chromium

# 6) Скопируем остальные файлы бота в контейнер
COPY . /app

# 7) Запуск бота, предполагая, что главный файл - grokcian.py
CMD ["python3", "grokcian.py"]
