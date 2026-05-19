#!/usr/bin/env python3
"""
Load Test Suite for FlinkTech Crypto Price Processing Pipeline
Generates realistic crypto price data and sends it to Kafka and Socket sources.
"""

import json
import random
import time
import threading
import socket
import sys
from datetime import datetime

try:
    from kafka import KafkaProducer
except ImportError:
    print("Installing kafka-python...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "kafka-python"])
    from kafka import KafkaProducer

class CryptoPriceGenerator:
    def __init__(self):
        self.base_prices = {
            'BTC': 65000.0, 'ETH': 3500.0, 'SOL': 150.0, 'ADA': 0.55,
            'DOGE': 0.15, 'BNB': 600.0, 'XRP': 0.60, 'AVAX': 40.0,
            'DOT': 7.5, 'MATIC': 0.85, 'LINK': 15.0, 'LTC': 85.0
        }
        self.current_prices = dict(self.base_prices)
        self.volatility = {
            'BTC': 0.002, 'ETH': 0.003, 'SOL': 0.005, 'ADA': 0.008,
            'DOGE': 0.010, 'BNB': 0.003, 'XRP': 0.007, 'AVAX': 0.006,
            'DOT': 0.005, 'MATIC': 0.008, 'LINK': 0.004, 'LTC': 0.004
        }
        self.message_count = 0
        self.start_time = time.time()

    def generate_price(self, symbol):
        base = self.current_prices.get(symbol, self.base_prices.get(symbol, 100.0))
        vol = self.volatility.get(symbol, 0.005)
        change = random.gauss(0, vol * base)
        new_price = base + change
        self.current_prices[symbol] = max(0.001, new_price)
        
        if random.random() < 0.01:
            new_price *= random.uniform(1.1, 1.5)
        
        return round(new_price, 2)

    def generate_message(self, symbol=None):
        if symbol is None:
            symbol = random.choice(list(self.base_prices.keys()))
        
        price = self.generate_price(symbol)
        volume = round(random.uniform(100, 100000), 2)
        timestamp = int(time.time() * 1000)
        
        self.message_count += 1
        return f"{symbol},{price},{timestamp},{volume}"

class LoadTester:
    def __init__(self, kafka_broker='localhost:29092', socket_host='localhost', socket_port=9999):
        self.kafka_broker = kafka_broker
        self.socket_host = socket_host
        self.socket_port = socket_port
        self.generator = CryptoPriceGenerator()
        self.running = False
        self.stats = {'kafka_sent': 0, 'socket_sent': 0, 'errors': 0}

    def kafka_load_test(self, messages_per_second=50, duration_seconds=300):
        print(f"Starting Kafka load test: {messages_per_second} msg/s for {duration_seconds}s")
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_broker,
                value_serializer=lambda x: x.encode('utf-8'),
                acks='all',
                retries=3
            )
            
            start_time = time.time()
            end_time = start_time + duration_seconds
            interval = 1.0 / messages_per_second
            
            while time.time() < end_time and self.running:
                message = self.generator.generate_message()
                symbol = message.split(',')[0]
                
                producer.send('crypto-prices', value=message, key=symbol.encode())
                self.stats['kafka_sent'] += 1
                
                if self.stats['kafka_sent'] % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = self.stats['kafka_sent'] / elapsed if elapsed > 0 else 0
                    print(f"Kafka: {self.stats['kafka_sent']} messages sent ({rate:.1f} msg/s)")
                
                time.sleep(interval)
            
            producer.flush()
            producer.close()
            print(f"Kafka load test completed: {self.stats['kafka_sent']} messages")
            
        except Exception as e:
            print(f"Kafka load test error: {e}")
            self.stats['errors'] += 1

    def socket_load_test(self, messages_per_second=20, duration_seconds=300):
        print(f"Starting Socket load test: {messages_per_second} msg/s for {duration_seconds}s")
        start_time = time.time()
        end_time = start_time + duration_seconds
        interval = 1.0 / messages_per_second
        reconnect_count = 0
        max_reconnects = 30
        
        while time.time() < end_time and self.running and reconnect_count < max_reconnects:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.settimeout(5)
                sock.connect((self.socket_host, self.socket_port))
                reconnect_count += 1
                print(f"Socket connected to Flink (reconnect #{reconnect_count})")
                
                batch = []
                batch_size = 5
                last_flush = time.time()
                
                while time.time() < end_time and self.running:
                    message = self.generator.generate_message() + '\n'
                    batch.append(message)
                    
                    if len(batch) >= batch_size or (time.time() - last_flush) > 1.0:
                        try:
                            sock.sendall(''.join(batch).encode())
                            self.stats['socket_sent'] += len(batch)
                            batch = []
                            last_flush = time.time()
                        except (BrokenPipeError, ConnectionResetError):
                            break
                    
                    if self.stats['socket_sent'] % 50 == 0 and self.stats['socket_sent'] > 0:
                        elapsed = time.time() - start_time
                        rate = self.stats['socket_sent'] / elapsed if elapsed > 0 else 0
                        print(f"Socket: {self.stats['socket_sent']} messages sent ({rate:.1f} msg/s)")
                    
                    time.sleep(interval)
                
                if batch:
                    try:
                        sock.sendall(''.join(batch).encode())
                        self.stats['socket_sent'] += len(batch)
                    except:
                        pass
                
                sock.close()
                print(f"Socket connection closed normally ({self.stats['socket_sent']} total)")
                break
                
            except (ConnectionRefusedError, BrokenPipeError, ConnectionResetError) as e:
                print(f"Socket connection lost: {e}, retrying in 2s...")
                time.sleep(2)
            except Exception as e:
                print(f"Socket load test error: {e}")
                self.stats['errors'] += 1
                break
        
        if reconnect_count >= max_reconnects:
            print(f"Socket: Max reconnections ({max_reconnects}) reached. Note: Socket source is for dev/testing only. Use Kafka for production.")

    def run_spike_test(self):
        print("Running spike test: sudden burst of high-price alerts...")
        symbols = ['BTC', 'ETH', 'SOL']
        for symbol in symbols:
            for i in range(20):
                price = self.generator.base_prices[symbol] * random.uniform(1.2, 2.0)
                timestamp = int(time.time() * 1000)
                message = f"{symbol},{price:.2f},{timestamp},{random.uniform(1000, 50000):.2f}"
                
                try:
                    producer = KafkaProducer(
                        bootstrap_servers=self.kafka_broker,
                        value_serializer=lambda x: x.encode('utf-8')
                    )
                    producer.send('crypto-prices', value=message, key=symbol.encode())
                    producer.flush()
                    producer.close()
                except:
                    pass
                time.sleep(0.1)
        print("Spike test completed")

    def run(self, kafka_rate=50, socket_rate=20, duration=300, include_spike=True):
        self.running = True
        
        kafka_thread = threading.Thread(target=self.kafka_load_test, args=(kafka_rate, duration))
        socket_thread = threading.Thread(target=self.socket_load_test, args=(socket_rate, duration))
        
        kafka_thread.start()
        socket_thread.start()
        
        if include_spike:
            time.sleep(30)
            self.run_spike_test()
        
        kafka_thread.join()
        socket_thread.join()
        
        total_time = time.time() - self.start_time
        total_messages = self.stats['kafka_sent'] + self.stats['socket_sent']
        
        print("\n" + "="*60)
        print("LOAD TEST RESULTS")
        print("="*60)
        print(f"Total Duration: {total_time:.1f}s")
        print(f"Total Messages: {total_messages}")
        print(f"Kafka Messages: {self.stats['kafka_sent']}")
        print(f"Socket Messages: {self.stats['socket_sent']}")
        print(f"Errors: {self.stats['errors']}")
        print(f"Average Rate: {total_messages/total_time:.1f} msg/s")
        print("="*60)

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='FlinkTech Load Tester')
    parser.add_argument('--kafka-rate', type=int, default=50, help='Kafka messages per second')
    parser.add_argument('--socket-rate', type=int, default=20, help='Socket messages per second')
    parser.add_argument('--duration', type=int, default=300, help='Test duration in seconds')
    parser.add_argument('--no-spike', action='store_true', help='Skip spike test')
    parser.add_argument('--quick', action='store_true', help='Quick test: 60s at 10 msg/s')
    
    args = parser.parse_args()
    
    if args.quick:
        args.kafka_rate = 10
        args.socket_rate = 5
        args.duration = 60
    
    tester = LoadTester()
    tester.start_time = time.time()
    tester.run(
        kafka_rate=args.kafka_rate,
        socket_rate=args.socket_rate,
        duration=args.duration,
        include_spike=not args.no_spike
    )
