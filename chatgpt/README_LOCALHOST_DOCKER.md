# Formula 1 Localhost Docker Project

Project ini disusun ulang dari `Formula_1-main.zip` agar pipeline historis dan realtime bisa dijalankan secara lokal dengan Docker. Struktur asli tetap dipertahankan sejauh mungkin, tetapi bagian yang sebelumnya mengarah ke cloud, IP EC2, Confluent Cloud, dan path `/home/ubuntu` sudah dipatch menjadi konfigurasi localhost.

## 1. Arsitektur yang Dibuat

Historical pipeline:

```text
OpenF1 API
  -> Airflow 3.0.6 DAG
  -> DockerOperator menjalankan image f1-python-etl:local
  -> Python extract/load script
  -> Snowflake raw/staging tables
  -> DockerOperator menjalankan image f1-dbt-snowflake:local
  -> dbt Snowflake models
  -> Snowflake final marts
  -> Grafana historical dashboard template
```

Realtime pipeline:

```text
OpenF1 API
  -> Flask API /run-and-redirect
  -> Kafka producer Python script
  -> Kafka topic_0, topic_1, topic_2, topic_3
  -> Native SingleStore Pipeline
  -> SingleStore realtime tables
  -> Grafana realtime dashboard
```

Catatan penting: Airflow, dbt container, Kafka, Kafka UI, SingleStore, Flask API, dan Grafana berjalan di Docker localhost. Snowflake tetap layanan cloud karena pipeline historis memang memakai `dbt-snowflake` dan koneksi Snowflake, bukan database lokal.

## 2. Port Localhost

| Service | URL | Login default |
|---|---|---|
| Airflow API Server/UI | http://localhost:8080 | airflow / airflow |
| Grafana | http://localhost:3000 | admin / admin |
| Kafka UI | http://localhost:8082 | tidak perlu login |
| SingleStore Studio | http://localhost:8081 | root / rootpass |
| SingleStore SQL | localhost:3306 | root / rootpass |
| Flask realtime trigger | http://localhost:5000 | tidak perlu login |

## 3. Persiapan Pertama

Pastikan Docker Desktop sudah berjalan. Untuk Windows, lebih aman jalankan terminal sebagai PowerShell biasa di folder project.

Salin file environment:

```powershell
copy .env.example .env
```

Isi `LOCAL_PROJECT_PATH` di file `.env` menggunakan path absolut folder project. Contoh Windows:

```env
LOCAL_PROJECT_PATH=C:/Users/LENOVO/formula1-localhost-project
```

Contoh Linux/macOS:

```env
LOCAL_PROJECT_PATH=/home/user/formula1-localhost-project
```

Jika ingin menjalankan historical pipeline, isi juga variabel Snowflake:

```env
SNOWFLAKE_ACCOUNT=xxxxx.region
SNOWFLAKE_USER=xxxxx
SNOWFLAKE_PASSWORD=xxxxx
SNOWFLAKE_ROLE=xxxxx
SNOWFLAKE_DATABASE=xxxxx
SNOWFLAKE_WAREHOUSE=xxxxx
SNOWFLAKE_SCHEMA=HITESH
```

Jangan commit file `.env` ke GitHub karena berisi credential.

## 4. Build Image Docker

Jalankan:

```powershell
docker compose --profile images build
```

Perintah ini membangun image berikut:

```text
f1-airflow:3.0.6-local
f1-python-etl:local
f1-dbt-snowflake:local
f1-realtime-api:local
```

Image Airflow dan dbt sengaja dipisah. Airflow hanya bertugas orkestrasi, sedangkan `dbt-snowflake` berjalan di container terpisah melalui DockerOperator.

## 5. Inisialisasi Airflow

Jalankan:

```powershell
docker compose up airflow-init
```

Jika selesai tanpa error besar, lanjutkan menyalakan semua service:

```powershell
docker compose up -d
```

Cek status:

```powershell
docker compose ps
```

## 6. Menjalankan Realtime Pipeline

Buka Kafka UI di:

```text
http://localhost:8082
```

Pastikan topic berikut muncul:

```text
topic_0  -> location
topic_1  -> car_data
topic_2  -> intervals
topic_3  -> position
```

Jalankan trigger realtime dari browser atau PowerShell:

```powershell
Invoke-WebRequest "http://localhost:5000/run-and-redirect?start=2024-03-02T15:00:00&session=9158&driver=55&data_types=car,location&minutes=5" -UseBasicParsing
```

Alternatif browser:

```text
http://localhost:5000/run-and-redirect?start=2024-03-02T15:00:00&session=9158&driver=55&data_types=car,location&minutes=5
```

Setelah itu buka Grafana:

```text
http://localhost:3000
```

Masuk ke folder `Formula 1 Local`, lalu buka dashboard `F1 Realtime Local`.

Untuk cek data langsung dari SingleStore:

```powershell
docker exec -it f1-singlestore singlestore -uroot -prootpass < scripts/check_realtime.sql
```

Jika perintah redirect belum menampilkan data, cek log Flask producer:

```powershell
docker compose logs -f realtime-api
```

## 7. Menjalankan Historical Pipeline di Airflow

Buka Airflow:

```text
http://localhost:8080
```

Login:

```text
username: airflow
password: airflow
```

Aktifkan DAG:

```text
f1_historical_snowflake_dbt_local
```

Trigger DAG dengan konfigurasi contoh:

```json
{
  "year": 2024,
  "limit_meetings": 1,
  "limit_sessions": 1,
  "endpoints": "drivers,laps,pit,position,intervals,stints,weather"
}
```

Jika ingin mengambil satu session tertentu:

```json
{
  "meeting_key": "latest",
  "session_key": "latest",
  "limit_meetings": 1,
  "limit_sessions": 1,
  "endpoints": "drivers,laps,pit,position,intervals,stints,weather"
}
```

Untuk `car_data`, aktifkan manual lewat command ETL karena datanya besar. Di DAG, default `car_data` tidak diaktifkan agar proses lokal tidak terlalu berat.

## 8. Historical Dashboard Snowflake

Dashboard realtime otomatis aktif karena memakai datasource MySQL ke SingleStore. Untuk historical dashboard, file template tersedia di:

```text
grafana/dashboards/f1_historical_snowflake_template.json.disabled
```

Self-managed Grafana OSS tidak otomatis menyediakan Snowflake datasource. Jika memakai Grafana Enterprise atau Grafana Cloud dengan Snowflake plugin, ubah nama file menjadi `.json`, konfigurasi datasource UID `snowflake_historical`, lalu restart Grafana:

```powershell
docker compose restart grafana
```

## 9. Perintah Maintenance

Melihat log semua service:

```powershell
docker compose logs -f --tail=200
```

Stop container tanpa hapus volume:

```powershell
docker compose down
```

Reset total termasuk volume database:

```powershell
docker compose down -v
```

Build ulang setelah edit Dockerfile:

```powershell
docker compose --profile images build --no-cache
```

## 10. Catatan Perubahan dari Repo Asli

1. DAG lama dipindahkan ke `dags/_legacy_disabled` karena masih memakai pola lama dan dependency yang tidak lengkap untuk Airflow lokal.
2. DAG baru `f1_local_dockeroperator_dag.py` dibuat khusus untuk Airflow 3.0.6 dan DockerOperator.
3. `dbt-snowflake` dipisah ke image `f1-dbt-snowflake:local`.
4. Script Flask tidak lagi redirect ke IP EC2, tetapi ke `http://localhost:3000`.
5. Kafka producer tidak lagi memakai konfigurasi Confluent Cloud kosong, tetapi memakai `kafka:29092` di jaringan Docker.
6. SingleStore memakai native pipeline dari Kafka, bukan consumer manual Python.
7. Credential Snowflake lama dari repo tidak digunakan. Semua koneksi dipindah ke environment variable.

## 11. Troubleshooting Singkat

Jika Airflow task DockerOperator error `Mounts denied` atau `bind source path does not exist`, periksa `LOCAL_PROJECT_PATH` di `.env`. Gunakan path absolut dan ubah backslash Windows menjadi slash.

Jika Kafka UI kosong, jalankan:

```powershell
docker compose logs kafka-init
```

Jika data Kafka masuk tetapi SingleStore kosong, restart inisialisasi pipeline:

```powershell
docker compose up singlestore-init
```

Jika Grafana tidak menampilkan data, pastikan tabel SingleStore sudah terisi:

```powershell
docker exec -it f1-singlestore singlestore -uroot -prootpass -e "USE f1_realtime; SELECT COUNT(*) FROM f1_car_data;"
```
