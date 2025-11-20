# ==============================================================================
# Dockerfile - Custom Airflow Image với Selenium Support
# ==============================================================================
#
# Base Image: apache/airflow:2.8.1
# Custom additions:
#   - PostgreSQL development libraries
#   - Build tools (gcc, g++, make)
#   - Chromium browser + ChromeDriver (for Selenium)
#   - Python packages from requirements.txt
#
# Purpose:
#   Tạo image có đầy đủ dependencies để crawl Lazada bằng Selenium
#   và kết nối với PostgreSQL database.
#
# Build: docker-compose build
# Image name: lazada_tiki_airflow:latest
#
# ==============================================================================

# Base image: Official Airflow 2.8.1 với Python 3.10
FROM apache/airflow:2.8.1

# ==============================================================================
# SYSTEM PACKAGES - Cài đặt dependencies ở system level
# ==============================================================================

# Switch to root user để có quyền cài đặt system packages
USER root

# Cài đặt các packages cần thiết:
#   - libpq-dev: PostgreSQL client library (để build psycopg2)
#   - build-essential: Compiler tools (gcc, g++, make) cho build Python packages
#   - wget: Download tool
#   - gnupg: GPG tools
#   - fonts-liberation: Fonts cho headless Chrome
#   - libxss1, libayatana-appindicator1: Dependencies cho Chrome
#   - chromium: Headless browser cho Selenium
#   - chromium-driver: WebDriver cho Chromium
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        # PostgreSQL dependencies
        libpq-dev \
        # Build tools
        build-essential \
        wget \
        gnupg \
        # Chrome dependencies
        fonts-liberation \
        libxss1 \
        libayatana-appindicator1 \
        libayatana-indicator7 \
        # Selenium dependencies
        chromium \
        chromium-driver \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ==============================================================================
# PYTHON PACKAGES - Cài đặt Python dependencies
# ==============================================================================

# Switch về airflow user (best practice: không chạy app với root)
USER airflow

# Copy requirements.txt vào container
# File này chứa các Python packages cần thiết cho ETL pipeline
COPY requirements.txt .

# Cài đặt Python packages:
#   --no-cache-dir: Không lưu cache để giảm image size
#   -r requirements.txt: Install tất cả packages trong file
# 
# Packages thường có trong requirements.txt:
#   - selenium: Web automation cho Lazada crawling
#   - requests: HTTP client cho Tiki API
#   - psycopg2-binary: PostgreSQL adapter
#   - apache-airflow-providers-postgres: Airflow Postgres integration
#   - apache-airflow-providers-slack: Airflow Slack integration
RUN pip install --no-cache-dir -r requirements.txt

# ==============================================================================
# ENVIRONMENT VARIABLES (Optional)
# ==============================================================================
# ChromeDriver path - có thể override bằng docker-compose environment
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# ==============================================================================
# NOTES
# ==============================================================================
# 1. Image size: ~2-3GB do có Chrome và ChromeDriver
# 2. ChromeDriver location: /usr/bin/chromedriver
# 3. Để test Selenium local, dùng: docker run -it <image> bash
# 4. Logs location: /opt/airflow/logs (mounted từ host)
