FROM bitnamilegacy/spark:3.5.1

USER root
# Tiện ích tải file + Python (ảnh này đã có Python, thêm pip nếu cần)
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*


# Cài Python libs cho job
COPY ./requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# (Tùy chọn) copy sẵn conf vào image — vẫn có thể override bằng volume
# COPY spark/conf/ /opt/bitnami/spark/conf/

# Trả quyền chạy về user không đặc quyền (theo chuẩn bitnami)
USER 1001
