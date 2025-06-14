# 🛍️ Retail Data Analytics Platform

---

## 🧭 Mục lục
- [Giới thiệu](#giới-thiệu)
- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Công nghệ sử dụng](#công-nghệ-sử-dụng)
- [Hướng dẫn triển khai](#hướng-dẫn-triển-khai)
- [Tài khoản mặc định](#tài-khoản-mặc-định)
- [Ghi chú](#ghi-chú)

---

## 🎯 Giới thiệu

Dự án xây dựng hệ thống end-to-end cho phép:
- Thu thập dữ liệu bán lẻ từ nhiều nguồn (thủ công hoặc tự động).
- Lưu trữ dữ liệu trên GCS.
- Xử lý bằng BigQuery, theo các layer: landing → staging → modeling.
- Kiểm thử chất lượng dữ liệu bằng Soda.
- Quản lý metadata và lineage bằng DataHub.
- Trực quan hóa dữ liệu bằng Apache Superset.

---

## 🏗️ Kiến trúc hệ thống

![Kiến trúc hệ thống](images/architecture.png)

---

## ⚙️ Công nghệ sử dụng

- Apache Airflow 2.8.1
- Google Cloud Storage
- BigQuery
- Apache Superset
- DataHub
- Soda
- Docker, Docker Compose
- Python, Pandas

---

## 🚀 Hướng dẫn triển khai

### ✅ Bước 1: Clone repository

```bash
git clone https://github.com/tungnt763/DATN-Retailing.git
cd DATN-Retailing
```

---

### ⚙️ Bước 2: Thiết lập Apache Airflow

#### Thiết lập biến môi trường

Tạo file `.env` hoặc export trực tiếp:

```bash
OPENEXCHANGE_API_KEY=<openexchange_key>
AIRFLOW__SMTP__SMTP_USER=<user_email>
AIRFLOW__SMTP__SMTP_PASSWORD=<password>
GOOGLE_APPLICATION_CREDENTIALS=<absolute_path_to_service_account.json>
```

#### Build và khởi động Airflow

```bash
docker compose build
docker compose up -d
```

---

### 🔌 Bước 3: Tạo kết nối trong Airflow

Vào giao diện Airflow: [http://localhost:8080](http://localhost:8080)

Tạo các connection sau:

1. **GCP Connection**

   - Conn ID: `gcp`
   - Conn Type: `Google Cloud`
   - Keyfile Path: `path/to/service_account.json`
   - Retries: `5`

2. **DataHub Connection**

   - Conn ID: `datahub_rest_default`
   - Conn Type: `datahub-rest`
   - Host: `http://datahub-gms:8080`

---

### 📚 Bước 4: Triển khai DataHub

```bash
pip install --upgrade pip wheel setuptools
pip install --upgrade acryl-datahub
datahub version
datahub docker quickstart --quickstart-compose-file DATN-Retailing/datahub/docker-compose.yaml
```

---

### 📊 Bước 5: Triển khai Apache Superset

```bash
git clone https://github.com/apache/superset
cd superset
docker build -f Dockerfile.bigquery -t superset-bigquery:latest .
docker compose -f docker-compose-image-tag.yml up -d
```

---

### 🔗 Bước 6: Kết nối Superset với BigQuery

- Truy cập Superset tại [http://localhost:8088](http://localhost:8088)
- Thêm kết nối BigQuery sử dụng `service_account.json`
- Chạy các file SQL trong thư mục `visualization/` để tạo dataset và dashboard

---

### 🧲 Bước 7: Cấu hình kiểm thử dữ liệu với Soda

#### 1. Tạo tài khoản Soda Cloud & lấy API Key

- Truy cập: [https://cloud.soda.io](https://cloud.soda.io)

#### 2. Tạo file config cho từng layer trong `include/soda/<layer_name>/soda.yml`

```yaml
data_source retail_<layer_name>:
  type: bigquery
  connection:
    account_info_json_path: <path_to_service_account.json>
    auth_scopes:
      - https://www.googleapis.com/auth/bigquery
      - https://www.googleapis.com/auth/cloud-platform
      - https://www.googleapis.com/auth/drive
    project_id: <your_project_id>
    dataset: <layer_name>

soda_cloud:
  host: cloud.soda.io
  api_key_id: <your_soda_api_key_id>
  api_key_secret: <your_soda_api_key_secret>
```

---

### ⚡ Bước 8: Thiết lập Cloud Pub/Sub và mở cổng Airflow bằng Ngrok

#### 8.1 Tạo trigger Pub/Sub từ GCS

```bash
gsutil notification create -t gcs-trigger-topic \
  -f json -e OBJECT_FINALIZE gs://your-bucket-name
```

#### 8.2 Mở cổng Airflow bằng Ngrok

```bash
brew install --cask ngrok
ngrok config add-authtoken <your_ngrok_token>
ngrok http http://localhost:8080
```

Ghi lại địa chỉ như `https://<random>.ngrok.io` để sử dụng cho bước kế tiếp.

### 🌐 Bước 9: Thiết lập Web Application

#### 9.1 Clone repository WebApp

```bash
git clone https://github.com/tungnt763/DATN-WebApp.git
cd DATN-WebApp
```

#### 9.2 Thiết lập Backend (Flask)

1. Tạo Client ID trên GCP:
   - Truy cập Google Cloud Console → OAuth 2.0 Credentials
   - Tạo OAuth 2.0 Client ID cho ứng dụng web
   - Authorized redirect URI: `http://localhost:3000/login`, `http://localhost:5050/login/google/authorized`

2. Cấu hình môi trường backend:
   ```bash
   cd backend
   touch .env
   ```

   Nội dung file `.env`:
   ```
   GCP_BUCKET_NAME=retailing_data
   GCP_CREDENTIAL_PATH=/path/to/your/service_account.json
   DATABASE_URL=postgresql://airflow:airflow@localhost:5432/airflow
   FLASK_SECRET_KEY=your_flask_secret_key
   GOOGLE_CLIENT_ID=your_google_client_id
   GOOGLE_CLIENT_SECRET=your_google_client_secret
   ALLOWED_GOOGLE_DOMAINS=@gmail.com
   ```

3. Chạy ứng dụng backend:
   ```bash
   flask run --host=0.0.0.0 --port=5050
   ```

#### 9.3 Thiết lập Frontend (React)

1. Cấu hình môi trường:
   ```bash
   cd ../frontend
   touch .env
   ```

   Nội dung `.env`:
   ```
   REACT_APP_GOOGLE_CLIENT_ID=your_google_client_id
   ```

2. Chạy giao diện web:
   ```bash
   npm install
   npm start
   ```

   Giao diện sẽ chạy tại `http://localhost:3000`

#### 9.4 Cấu hình Cloud Function

1. Tạo file cấu hình:
   ```bash
   cd ../cloud-function
   touch .env.yaml
   ```

2. Nội dung file `.env.yaml`:
   ```yaml
   AIRFLOW_API_URL: "https://<ngrok_id>.ngrok.io/api/v1/dags"
   AIRFLOW_API_TOKEN: ""
   USE_BASIC_AUTH: "true"
   BASIC_AUTH_USER: "admin"
   BASIC_AUTH_PASS: "admin"
   ```

3. Triển khai Cloud Function:
   ```bash
   gcloud functions deploy gcs_trigger_dag \
     --runtime python310 \
     --entry-point trigger_dag \
     --trigger-topic gcs-trigger-topic \
     --env-vars-file .env.yaml \
     --region asia-southeast1
   ```

> 💡 Cloud Function sẽ được gọi mỗi khi có file mới đến GCS để kích hoạt DAG tương ứng qua Airflow API.

#### 9.5 Mô phỏng phát sinh dữ liệu POS

Để mô phỏng việc phát sinh dữ liệu từ hệ thống POS:

```bash
cd backend
python pos_sender/pos_sender.py
```

Script này sẽ tự động gửi các file `.csv` lên GCS định kỳ (mỗi 10 giây hoặc tùy chỉnh), giả lập quá trình phát sinh giao dịch từ hệ thống POS thực tế.

---

## 📁 Cấu trúc thư mục

```
DATN-Retailing/
├── dags/                       # Airflow DAGs
├── datahub/                    # Cấu hình DataHub
├── include/
│   └── soda/<layer_name>/      # File cấu hình kiểm thử dữ liệu
├── superset/                   # Cấu hình Superset
├── visualization/              # SQL tạo dashboard
├── docker-compose.yml
├── Dockerfile
└── README.md
```

---

## 📝 Lưu ý quan trọng

1. Đảm bảo tất cả các port cần thiết đã được mở:
   - Airflow: 8082
   - Superset: 8088
   - DataHub: 9002

2. Kiểm tra kết nối giữa các service:
   ```bash
   # Kiểm tra kết nối Airflow với GCS
   airflow connections test gcp
   
   # Kiểm tra kết nối DataHub
   airflow connections test datahub_rest_default
   ```

3. Backup dữ liệu quan trọng:
   ```bash
   # Backup Airflow metadata
   docker-compose exec postgres pg_dump -U airflow airflow > airflow_backup.sql
   
   # Backup Superset metadata
   cd superset 
   docker-compose -f docker-compose-image-tag.yaml exec superset superset db backup
   ```

---

## 🔐 Tài khoản mặc định

| Hệ thống         | Đường dẫn              | Username | Password |
|------------------|------------------------|----------|----------|
| Apache Airflow   | http://localhost:8082  | admin    | admin    |
| Superset         | http://localhost:8088  | admin    | admin    |
| DataHub          | http://localhost:9002  | datahub  | datahub  |

---

## 📌 Ghi chú

- Pipeline hỗ trợ kiểm thử theo layer: landing → staging → modeling
- Có hỗ trợ trigger DAG bằng Google Cloud Pub/Sub, Google Function khi file mới đến

---