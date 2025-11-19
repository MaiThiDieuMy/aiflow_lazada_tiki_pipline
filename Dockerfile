# Bắt đầu từ image Aiflow chính thức
FROM apache/airflow:2.8.1

# Chuyển sang user root để cài đặt các gói bổ sung
USER root

# Cập nhật hệ thống và cài đặt các gói bổ sung cần thiết
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq-dev \
    build-essential \
    wget \
    gnupg \
    fonts-liberation \
    libxss1 libayatana-appindicator1 libayatana-indicator7 \
    chromium \
    chromium-driver \
    && apt-get clean

# Chuyển sang user aiflow để chạy các lệnh liên quan đến Airflow
USER airflow

# Cài đặt các gói Python bổ sung cần thiết cho Airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

