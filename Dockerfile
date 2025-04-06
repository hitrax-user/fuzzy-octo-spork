# 1) Базовый образ с Python 3.11 (slim-версия для уменьшения размера)
FROM python:3.11-slim

# 2) Устанавливаем системные библиотеки, необходимые для Chromium и Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Основные зависимости для Chromium
    libnss3 \
    libxss1 \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libx11-xcb1 \
    libxcb-dri3-0 \
    libxcomposite1 \
    libxdamage1 \
    libxkbcommon0 \
    libxrandr2 \
    libpango-1.0-0 \
    libcairo2 \
    libfontconfig1 \
    libfreetype6 \
    # Дополнительные утилиты для установки
    wget \
    gnupg \
    apt-transport-https \
    && rm -rf /var/lib/apt/lists/*  # Очищаем кэш apt для уменьшения размера образа

# 3) Устанавливаем рабочую директорию
WORKDIR /app

# 4) Копируем все файлы проекта в контейнер
COPY . /app

# 5) Устанавливаем Python-зависимости и Playwright с Chromium
RUN pip3 install --no-cache-dir -r requirements.txt \
    && pip3 install --no-cache-dir playwright \
    && playwright install --with-deps chromium

# 6) Указываем команду для запуска бота
CMD ["python3", "grokcian.py"]