import logging
import os
import subprocess
from datetime import datetime, timedelta
from flask import Flask, request, redirect, jsonify

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)


def parse_start(raw_start: str) -> datetime:
    if not raw_start:
        raw_start = "2024-03-02T15:00:00"
    raw_start = raw_start.split(" ")[0].replace("Z", "")
    if "." in raw_start:
        raw_start = raw_start.split(".")[0]
    return datetime.strptime(raw_start, "%Y-%m-%dT%H:%M:%S")


@app.get("/")
def index():
    return jsonify({
        "service": "F1 realtime trigger",
        "example": "/run-and-redirect?start=2024-03-02T15:00:00&session=9158&driver=55&data_types=car,location",
    })


@app.route('/run-and-redirect', methods=['GET'])
def run_script():
    try:
        start_dt = parse_start(request.args.get('start'))
        minutes = int(request.args.get('minutes', '5'))
        end_dt = start_dt + timedelta(minutes=minutes)

        start_time = start_dt.strftime('%Y-%m-%dT%H:%M:%S.200')
        end_time = end_dt.strftime('%Y-%m-%dT%H:%M:%S.200')
        session = request.args.get('session', '9158')
        driver = request.args.get('driver', '55')
        data_types = [x.strip() for x in request.args.get('data_types', 'car,location').split(',') if x.strip()]

        script_path = os.getenv('PRODUCER_SCRIPT', '/app/realtime/kafka_producer_v6_cloud.py')
        cmd = [
            'python', script_path,
            '--param1', start_time,
            '--param2', end_time,
            '--session', session,
            '--driver', driver,
            '--data-types', *data_types,
        ]

        logger.info("Running producer command: %s", ' '.join(cmd))
        subprocess.Popen(cmd, env=os.environ.copy())

        grafana_base_url = os.getenv('GRAFANA_BASE_URL', 'http://localhost:3000')
        redirect_url = (
            f'{grafana_base_url}/d/f1-realtime-local/f1-realtime-local'
            f'?orgId=1&from=now-30m&to=now&var-driver={driver}&refresh=1s'
        )
        return redirect(redirect_url)

    except Exception as e:
        logger.exception("Error while running producer")
        return {'error': str(e)}, 500


if __name__ == '__main__':
    logger.info("Starting Flask app on port 5000")
    app.run(host='0.0.0.0', port=5000)
