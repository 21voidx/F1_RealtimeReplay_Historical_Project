# Jalankan dari PowerShell setelah docker compose up -d selesai.
Invoke-WebRequest "http://localhost:5000/run-and-redirect?start=2024-03-02T15:00:00&session=9158&driver=55&data_types=car,location&minutes=5" -UseBasicParsing
