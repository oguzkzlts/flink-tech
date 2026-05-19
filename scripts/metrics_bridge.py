#!/usr/bin/env python3
"""
FlinkTech Metrics Bridge
Reads REAL metrics from Flink REST API and Kafka offsets, pushes to Prometheus Pushgateway
"""

import json
import time
import urllib.request
import urllib.error
import subprocess
from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway, Enum

FLINK_JOBMANAGER_URL = "http://localhost:8081"
PROMETHEUS_PUSHGATEWAY = "http://localhost:9091"
KAFKA_BROKER = "localhost:29092"
KAFKA_TOPIC = "crypto-prices"

class FlinkMetricsBridge:
    def __init__(self):
        self.registry = CollectorRegistry()
        
        self.messages_total = Counter('flintech_messages_total', 'Total messages processed', registry=self.registry)
        self.alerts_total = Counter('flintech_alerts_total', 'Total alerts generated', registry=self.registry)
        self.errors_total = Counter('flintech_errors_total', 'Total errors encountered', registry=self.registry)
        self.windows_processed = Counter('flintech_windows_processed_total', 'Total windows processed', registry=self.registry)
        
        self.job_status = Enum('flintech_job_status', 'Current job status', states=['RUNNING', 'FAILED', 'CANCELED', 'FINISHED'], registry=self.registry)
        self.task_count = Gauge('flintech_task_count', 'Number of running tasks', registry=self.registry)
        self.task_failed = Gauge('flintech_task_failed', 'Number of failed tasks', registry=self.registry)
        self.job_uptime = Gauge('flintech_job_uptime_seconds', 'Job uptime in seconds', registry=self.registry)
        self.kafka_offset = Gauge('flintech_kafka_offset_total', 'Total Kafka topic offset', registry=self.registry)
        self.messages_per_second = Gauge('flintech_messages_per_second', 'Current message throughput (msg/s)', registry=self.registry)
        
        self.last_metrics = {
            'kafka_offset': 0,
            'timestamp': time.time()
        }

    def get_job_id(self):
        try:
            url = f"{FLINK_JOBMANAGER_URL}/jobs/overview"
            with urllib.request.urlopen(url, timeout=5) as response:
                data = json.loads(response.read())
                if data['jobs']:
                    return data['jobs'][0]['jid']
        except Exception as e:
            print(f"Error getting job ID: {e}")
        return None

    def get_job_metrics(self, job_id):
        try:
            url = f"{FLINK_JOBMANAGER_URL}/jobs/{job_id}"
            with urllib.request.urlopen(url, timeout=5) as response:
                return json.loads(response.read())
        except Exception as e:
            print(f"Error getting job metrics: {e}")
            return None

    def get_kafka_offsets(self):
        try:
            result = subprocess.run(
                ['docker', 'exec', 'flinktech-kafka-1', 'kafka-run-class', 'kafka.tools.GetOffsetShell',
                 '--broker-list', 'localhost:9092', '--topic', KAFKA_TOPIC],
                capture_output=True, text=True, timeout=10
            )
            total_offset = 0
            for line in result.stdout.strip().split('\n'):
                if line and ':' in line:
                    parts = line.split(':')
                    if len(parts) >= 3:
                        total_offset += int(parts[2])
            return total_offset
        except Exception as e:
            print(f"Error getting Kafka offsets: {e}")
            return 0

    def update_metrics(self):
        job_id = self.get_job_id()
        if not job_id:
            return False
        
        job_data = self.get_job_metrics(job_id)
        if not job_data:
            return False
        
        tasks = job_data.get('tasks', {})
        self.task_count.set(tasks.get('running', 0))
        self.task_failed.set(tasks.get('failed', 0))
        
        status = job_data.get('state', 'UNKNOWN')
        if status in ['RUNNING', 'FAILED', 'CANCELED', 'FINISHED']:
            self.job_status.state(status)
        
        start_time = job_data.get('start-time', 0)
        if start_time > 0:
            uptime = (time.time() * 1000 - start_time) / 1000
            self.job_uptime.set(uptime)
        
        current_offset = self.get_kafka_offsets()
        self.kafka_offset.set(current_offset)
        
        current_time = time.time()
        time_diff = current_time - self.last_metrics['timestamp']
        
        if time_diff > 0:
            offset_diff = current_offset - self.last_metrics['kafka_offset']
            if offset_diff > 0:
                self.messages_total.inc(offset_diff)
                rate = offset_diff / time_diff
                self.messages_per_second.set(rate)
                print(f"Kafka offset: {current_offset} | +{offset_diff} msgs in {time_diff:.1f}s | Rate: {rate:.1f} msg/s")
        
        if tasks.get('failed', 0) > 0:
            self.errors_total.inc(tasks.get('failed', 0))
        
        self.last_metrics = {
            'kafka_offset': current_offset,
            'timestamp': current_time
        }
        
        return True

    def push_metrics(self):
        try:
            push_to_gateway(PROMETHEUS_PUSHGATEWAY, job='flinktech_metrics', registry=self.registry)
            return True
        except Exception as e:
            print(f"Error pushing metrics: {e}")
            return False

    def run(self, interval=5):
        print(f"Starting FlinkTech Metrics Bridge (interval: {interval}s)")
        print(f"Flink JobManager: {FLINK_JOBMANAGER_URL}")
        print(f"Prometheus Pushgateway: {PROMETHEUS_PUSHGATEWAY}")
        print(f"Kafka Broker: {KAFKA_BROKER}")
        
        self.last_metrics['kafka_offset'] = self.get_kafka_offsets()
        self.last_metrics['timestamp'] = time.time()
        print(f"Initial Kafka offset: {self.last_metrics['kafka_offset']}")
        
        while True:
            if self.update_metrics():
                if self.push_metrics():
                    print(f"Metrics pushed at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(interval)

if __name__ == '__main__':
    bridge = FlinkMetricsBridge()
    bridge.run()
