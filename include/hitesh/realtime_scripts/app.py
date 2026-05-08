import subprocess
from datetime import datetime, timedelta
import logging
from flask import Flask, request, redirect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/run-and-redirect', methods=['GET'])
def run_script():
    try:
        raw_start = request.args.get('start')
        start_time = raw_start.split(' ')[0]
        session = request.args.get('session')
        driver = request.args.get('driver', '55')

        start_dt = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S')
        end_dt = start_dt + timedelta(minutes=5)
        start_time = start_dt.strftime('%Y-%m-%dT%H:%M:%S.200')
        end_time = end_dt.strftime('%Y-%m-%dT%H:%M:%S.200')

        logger.info(f"Start time: {start_time}")
        logger.info(f"End time: {end_time}")
        logger.info(f"Session: {session}")
        logger.info(f"Driver: {driver}")

        # Command for car data
        cmd_car = [
            'python3',
            '/home/ubuntu/kafka_producer_v6_cloud.py',
            '--param1', start_time,
            '--param2', end_time,
            '--session', session,
            '--data-types', 'car',
            '--driver', driver
        ]

        # Command for location data
        cmd_location = [
            'python3',
            '/home/ubuntu/kafka_producer_v6_cloud.py',
            '--param1', start_time,
            '--param2', end_time,
            '--session', session,
            '--data-types', 'location',
            '--driver', driver
        ]

        logger.info(f"Running car command: {' '.join(cmd_car)}")
        subprocess.Popen(cmd_car)
        logger.info(f"Running location command: {' '.join(cmd_location)}")
        subprocess.Popen(cmd_location)

        redirect_url = f'http://13.60.222.132:3000/d/f1-telemetry/f1-real-time-telemetry?orgId=1&from={start_time.replace("T", " ")}&to={end_time.replace("T", " ")}&timezone=browser&var-driver={driver}&refresh=1s'
        return redirect(redirect_url)

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {'error': str(e)}, 500

if __name__ == '__main__':
    logger.info("Starting Flask app on port 5000")
    app.run(host='0.0.0.0', port=5000)