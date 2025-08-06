#!/usr/bin/env python3
"""
MQTT Publisher with Paho Client - Publishes data to 2000 endpoints every 30 seconds
Uses Eclipse Paho MQTT Python client for reliable MQTT communication
Enhanced with spread publishing to distribute load evenly over the interval
"""

import json
import logging
import random
import threading
import time
import signal
import sys
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import paho.mqtt.client as mqtt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('mqtt_publisher.log')
    ]
)
logger = logging.getLogger(__name__)


class MQTTDataPublisher:
    """
    High-performance MQTT publisher using the Eclipse Paho client.
    Publishes realistic sensor data to 2000 MQTT endpoints with configurable intervals.
    Supports both burst and spread publishing modes.
    """

    def __init__(self, broker_url: str = "mqtt://localhost:1883",
                 username: str = "admin", password: str = "admin",
                 client_id: Optional[str] = None):
        """
        Initialize the MQTT publisher with connection parameters.

        Args:
            broker_url: MQTT broker URL (e.g., mqtt://broker:1883)
            username: MQTT username for authentication
            password: MQTT password for authentication
            client_id: Custom client ID (auto-generated if None)
        """
        self.broker_url = broker_url
        self.username = username
        self.password = password
        self.client_id = client_id or f"mqtt_publisher_{int(time.time())}"

        # Connection settings
        self.connected = False
        self.connection_event = threading.Event()
        self.client: Optional[mqtt.Client] = None

        # Publisher settings
        self.num_endpoints = 2000
        self.publish_interval = 30  # seconds
        self.batch_size = 50  # endpoints per batch
        self.publish_qos = 1  # QoS level for publishing
        self.retain_messages = False
        self.spread_mode = True  # New: Enable spread publishing by default

        # Performance metrics
        self.publish_count = 0
        self.error_count = 0
        self.last_publish_time = 0
        self.metrics_lock = threading.Lock()

        # Shutdown handling
        self.shutdown_event = threading.Event()
        self.setup_signal_handlers()

        # Parse broker URL
        self._parse_broker_url()

    def _parse_broker_url(self):
        """Parse broker URL to extract host, port, and protocol."""
        try:
            parsed = urlparse(self.broker_url)
            self.host = parsed.hostname or "localhost"
            self.port = parsed.port or 1883
            self.use_tls = parsed.scheme == "mqtts"

            logger.info(f"Parsed broker URL - Host: {self.host}, Port: {self.port}, TLS: {self.use_tls}")
        except Exception as e:
            logger.error(f"Failed to parse broker URL {self.broker_url}: {e}")
            raise

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            self.shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def on_connect(self, client: mqtt.Client, userdata, flags, rc):
        """Callback for when the client receives a CONNACK response."""
        if rc == 0:
            logger.info(f"Successfully connected to MQTT broker: {self.broker_url}")
            logger.info(f"Client ID: {self.client_id}")
            logger.info(f"Session present: {flags.get('session present', False)}")
            self.connected = True
            self.connection_event.set()
        else:
            error_messages = {
                1: "Connection refused - incorrect protocol version",
                2: "Connection refused - invalid client identifier",
                3: "Connection refused - server unavailable",
                4: "Connection refused - bad username or password",
                5: "Connection refused - not authorised"
            }
            error_msg = error_messages.get(rc, f"Connection refused - unknown error code {rc}")
            logger.error(f"Failed to connect to MQTT broker: {error_msg}")
            self.connected = False

    def on_disconnect(self, client: mqtt.Client, userdata, rc):
        """Callback for when the client disconnects."""
        if rc != 0:
            logger.warning(f"Unexpected disconnection from MQTT broker (code: {rc})")
        else:
            logger.info("Cleanly disconnected from MQTT broker")

        self.connected = False
        self.connection_event.clear()

    def on_publish(self, client: mqtt.Client, userdata, mid):
        """Callback for when a message is successfully published."""
        with self.metrics_lock:
            self.publish_count += 1
        logger.debug(f"Message {mid} published successfully")

    def on_log(self, client: mqtt.Client, userdata, level, buf):
        """Callback for MQTT client logging."""
        if level == mqtt.MQTT_LOG_ERR:
            logger.error(f"MQTT Client Error: {buf}")
        elif level == mqtt.MQTT_LOG_WARNING:
            logger.warning(f"MQTT Client Warning: {buf}")
        elif level == mqtt.MQTT_LOG_DEBUG:
            logger.debug(f"MQTT Client Debug: {buf}")

    def connect(self) -> bool:
        """
        Establish connection to MQTT broker.

        Returns:
            bool: True if the connection is successful, False otherwise
        """
        try:
            # Create MQTT client
            self.client = mqtt.Client(client_id=self.client_id, clean_session=True)

            # Set callbacks
            self.client.on_connect = self.on_connect
            self.client.on_disconnect = self.on_disconnect
            self.client.on_publish = self.on_publish
            self.client.on_log = self.on_log

            # Configure authentication
            if self.username and self.password:
                self.client.username_pw_set(self.username, self.password)
                logger.info(f"Authentication configured for user: {self.username}")

            # Configure TLS if needed
            if self.use_tls:
                self.client.tls_set()
                self.client.tls_insecure_set(True)
                logger.info("TLS encryption enabled")

            # Set connection options
            self.client.max_inflight_messages_set(100)  # Limit inflight messages

            # Connect to broker
            logger.info(f"Connecting to MQTT broker at {self.host}:{self.port}")
            self.client.connect(self.host, self.port, keepalive=60)

            # Start network loop
            self.client.loop_start()

            # Wait for connection with timeout
            if self.connection_event.wait(timeout=10):
                logger.info("MQTT connection established successfully")
                return True
            else:
                logger.error("Connection timeout - failed to connect within 10 seconds")
                return False

        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            return False

    def disconnect(self):
        """Disconnect from MQTT broker gracefully."""
        if self.client and self.connected:
            logger.info("Disconnecting from MQTT broker...")
            self.client.loop_stop()
            self.client.disconnect()

            # Wait a bit for clean disconnection
            time.sleep(1)

        self.connected = False
        logger.info("Disconnected from MQTT broker")

    def generate_sensor_data(self, sensor_id: int) -> Dict[str, Any]:
        """
        Generate realistic sensor data for industrial applications.

        Args:
            sensor_id: Unique sensor identifier

        Returns:
            Dict containing sensor data
        """
        # Determine a sensor type based on ID for consistent data patterns
        sensor_type_map = {
            0: "temperature",
            1: "humidity",
            2: "pressure",
            3: "vibration",
            4: "voltage",
            5: "current",
            6: "power",
            7: "flow",
            8: "level",
            9: "speed"
        }

        sensor_type = sensor_type_map[sensor_id % 10]

        # Generate type-specific data with realistic ranges
        base_data = {
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "location": {
                "zone": f"Zone_{(sensor_id - 1) // 100 + 1}",
                "building": f"Building_{(sensor_id - 1) // 500 + 1}",
                "floor": ((sensor_id - 1) % 100) // 20 + 1,
                "room": f"Room_{sensor_id % 50 + 1}"
            },
            "metadata": {
                "unit_id": f"UNIT_{sensor_id:04d}",
                "manufacturer": random.choice(["Siemens", "ABB", "Schneider", "Honeywell", "Emerson"]),
                "model": f"Model_{sensor_type.upper()}_{random.randint(100, 999)}",
                "firmware_version": f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 99)}"
            }
        }

        # Add sensor-specific measurements
        if sensor_type == "temperature":
            base_data.update({
                "temperature": round(random.uniform(18.0, 35.0), 2),
                "temperature_setpoint": round(random.uniform(20.0, 25.0), 1),
                "unit": "°C",
                "alarm_high": 40.0,
                "alarm_low": 10.0
            })
        elif sensor_type == "humidity":
            base_data.update({
                "humidity": round(random.uniform(30.0, 80.0), 2),
                "humidity_setpoint": round(random.uniform(45.0, 55.0), 1),
                "unit": "%RH",
                "alarm_high": 85.0,
                "alarm_low": 25.0
            })
        elif sensor_type == "pressure":
            base_data.update({
                "pressure": round(random.uniform(980.0, 1020.0), 2),
                "pressure_setpoint": round(random.uniform(995.0, 1005.0), 1),
                "unit": "mbar",
                "alarm_high": 1050.0,
                "alarm_low": 950.0
            })
        elif sensor_type == "vibration":
            base_data.update({
                "vibration_rms": round(random.uniform(0.0, 10.0), 3),
                "vibration_peak": round(random.uniform(0.0, 15.0), 3),
                "frequency": round(random.uniform(10.0, 1000.0), 1),
                "unit": "mm/s",
                "alarm_high": 12.0
            })
        elif sensor_type == "voltage":
            base_data.update({
                "voltage_l1": round(random.uniform(220.0, 240.0), 1),
                "voltage_l2": round(random.uniform(220.0, 240.0), 1),
                "voltage_l3": round(random.uniform(220.0, 240.0), 1),
                "unit": "V",
                "alarm_high": 250.0,
                "alarm_low": 200.0
            })
        elif sensor_type == "current":
            base_data.update({
                "current_l1": round(random.uniform(0.5, 15.0), 2),
                "current_l2": round(random.uniform(0.5, 15.0), 2),
                "current_l3": round(random.uniform(0.5, 15.0), 2),
                "unit": "A",
                "alarm_high": 20.0
            })
        elif sensor_type == "power":
            base_data.update({
                "active_power": round(random.uniform(100.0, 3600.0), 1),
                "reactive_power": round(random.uniform(50.0, 1000.0), 1),
                "power_factor": round(random.uniform(0.7, 0.99), 3),
                "unit": "W",
                "efficiency": round(random.uniform(85.0, 98.0), 2)
            })
        elif sensor_type == "flow":
            base_data.update({
                "flow_rate": round(random.uniform(10.0, 100.0), 2),
                "flow_total": round(random.uniform(1000.0, 50000.0), 1),
                "unit": "L/min",
                "alarm_high": 120.0,
                "alarm_low": 5.0
            })
        elif sensor_type == "level":
            base_data.update({
                "level_percent": round(random.uniform(10.0, 90.0), 2),
                "level_absolute": round(random.uniform(0.5, 4.5), 2),
                "unit": "%",
                "alarm_high": 95.0,
                "alarm_low": 5.0
            })
        elif sensor_type == "speed":
            base_data.update({
                "speed_rpm": round(random.uniform(1000.0, 3000.0), 1),
                "speed_setpoint": round(random.uniform(1500.0, 2500.0), 1),
                "unit": "RPM",
                "alarm_high": 3500.0,
                "alarm_low": 500.0
            })

        # Add common status information
        status_options = ["OK", "WARNING", "ERROR", "MAINTENANCE", "OFFLINE"]
        weights = [0.7, 0.15, 0.05, 0.08, 0.02]  # Weighted probability

        base_data.update({
            "status": random.choices(status_options, weights=weights)[0],
            "quality": random.choice(["GOOD", "UNCERTAIN", "BAD"]),
            "communication_status": "ONLINE" if random.random() > 0.02 else "TIMEOUT",
            "last_maintenance": (datetime.now(timezone.utc).timestamp() - random.randint(86400, 2592000)) * 1000,
            # Last 1-30 days
            "next_maintenance": (datetime.now(timezone.utc).timestamp() + random.randint(86400, 7776000)) * 1000
            # Next 1-90 days
        })

        return base_data

    def publish_to_endpoint(self, sensor_id: int) -> bool:
        """
        Publish data to a single sensor endpoint.

        Args:
            sensor_id: Sensor ID to publish data for

        Returns:
            bool: True if published successfully, False otherwise
        """
        if not self.connected:
            logger.warning(f"Not connected to broker, skipping sensor {sensor_id}")
            return False

        try:
            # Generate topic and data
            topic = f"sensor/data/{sensor_id:04d}"
            data = self.generate_sensor_data(sensor_id)
            payload = json.dumps(data, separators=(',', ':'))  # Compact JSON

            # Publish message
            msg_info = self.client.publish(
                topic=topic,
                payload=payload,
                qos=self.publish_qos,
                retain=self.retain_messages
            )

            # Check if publish was successful
            if msg_info.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.debug(f"Published to {topic}: {len(payload)} bytes (MID: {msg_info.mid})")
                return True
            else:
                logger.error(f"Failed to publish to {topic}: {mqtt.error_string(msg_info.rc)}")
                with self.metrics_lock:
                    self.error_count += 1
                return False

        except Exception as e:
            logger.error(f"Exception publishing to sensor {sensor_id}: {e}")
            with self.metrics_lock:
                self.error_count += 1
            return False

    def publish_batch_threaded(self, sensor_ids: list) -> Dict[str, int]:
        """
        Publish data to a batch of sensors using threading.

        Args:
            sensor_ids: List of sensor IDs to publish to

        Returns:
            Dict with success and failure counts
        """
        results = {"success": 0, "failed": 0}

        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all publishing tasks
            future_to_sensor = {
                executor.submit(self.publish_to_endpoint, sensor_id): sensor_id
                for sensor_id in sensor_ids
            }

            # Collect results
            for future in as_completed(future_to_sensor):
                if future.result():
                    results["success"] += 1
                else:
                    results["failed"] += 1

        return results

    def publish_all_endpoints(self) -> Dict[str, Any]:
        """
        Publish data to all configured endpoints in batches (original burst mode).

        Returns:
            Dict with publishing statistics
        """
        start_time = time.time()
        total_success = 0
        total_failed = 0

        logger.info(f"Starting publish cycle for {self.num_endpoints} endpoints...")

        # Process in batches to avoid overwhelming the broker
        for batch_start in range(1, self.num_endpoints + 1, self.batch_size):
            if self.shutdown_event.is_set():
                logger.info("Shutdown requested, stopping publish cycle")
                break

            batch_end = min(batch_start + self.batch_size - 1, self.num_endpoints)
            sensor_ids = list(range(batch_start, batch_end + 1))

            logger.debug(f"Publishing batch {batch_start}-{batch_end}")

            # Publish batch
            batch_results = self.publish_batch_threaded(sensor_ids)
            total_success += batch_results["success"]
            total_failed += batch_results["failed"]

            # Small delay between batches to prevent overwhelming
            if batch_end < self.num_endpoints:
                time.sleep(0.1)

        elapsed_time = time.time() - start_time
        self.last_publish_time = elapsed_time

        # Log statistics
        logger.info(f"Publish cycle completed:")
        logger.info(f"  - Total endpoints: {self.num_endpoints}")
        logger.info(f"  - Successful: {total_success}")
        logger.info(f"  - Failed: {total_failed}")
        logger.info(f"  - Duration: {elapsed_time:.2f} seconds")
        logger.info(f"  - Rate: {total_success / elapsed_time:.1f} msg/sec")

        return {
            "total_endpoints": self.num_endpoints,
            "successful": total_success,
            "failed": total_failed,
            "duration": elapsed_time,
            "rate": total_success / elapsed_time if elapsed_time > 0 else 0
        }

    def publish_all_endpoints_spread(self) -> Dict[str, Any]:
        """
        Publish data to all configured endpoints spread evenly over the publish interval.

        Returns:
            Dict with publishing statistics
        """
        start_time = time.time()
        total_success = 0
        total_failed = 0

        # Calculate timing for spreading publications
        total_batches = (self.num_endpoints + self.batch_size - 1) // self.batch_size
        batch_interval = self.publish_interval / total_batches if total_batches > 1 else 0

        logger.info(f"Starting spread publish cycle for {self.num_endpoints} endpoints...")
        logger.info(f"Publishing {total_batches} batches over {self.publish_interval}s")
        logger.info(f"Batch interval: {batch_interval:.2f}s")

        # Process in batches with calculated delays
        for batch_num, batch_start in enumerate(range(1, self.num_endpoints + 1, self.batch_size)):
            if self.shutdown_event.is_set():
                logger.info("Shutdown requested, stopping publish cycle")
                break

            batch_end = min(batch_start + self.batch_size - 1, self.num_endpoints)
            sensor_ids = list(range(batch_start, batch_end + 1))

            # Calculate when this batch should be published
            expected_time = start_time + (batch_num * batch_interval)
            current_time = time.time()

            # Wait if we're ahead of schedule
            if current_time < expected_time:
                sleep_time = expected_time - current_time
                logger.debug(f"Batch {batch_num + 1}/{total_batches}: waiting {sleep_time:.2f}s before publishing")
                if self.shutdown_event.wait(timeout=sleep_time):
                    logger.info("Shutdown requested during batch delay")
                    break

            logger.debug(f"Publishing batch {batch_num + 1}/{total_batches} ({batch_start}-{batch_end})")

            # Publish batch
            batch_start_time = time.time()
            batch_results = self.publish_batch_threaded(sensor_ids)
            batch_duration = time.time() - batch_start_time

            total_success += batch_results["success"]
            total_failed += batch_results["failed"]

            logger.debug(f"Batch {batch_num + 1} completed in {batch_duration:.2f}s")

        elapsed_time = time.time() - start_time
        self.last_publish_time = elapsed_time

        # Log statistics
        logger.info(f"Spread publish cycle completed:")
        logger.info(f"  - Total endpoints: {self.num_endpoints}")
        logger.info(f"  - Successful: {total_success}")
        logger.info(f"  - Failed: {total_failed}")
        logger.info(f"  - Duration: {elapsed_time:.2f} seconds")
        logger.info(f"  - Rate: {total_success / elapsed_time:.1f} msg/sec")
        logger.info(f"  - Target interval: {self.publish_interval}s")
        logger.info(f"  - Spread efficiency: {min(elapsed_time / self.publish_interval, 1.0) * 100:.1f}%")

        return {
            "total_endpoints": self.num_endpoints,
            "successful": total_success,
            "failed": total_failed,
            "duration": elapsed_time,
            "rate": total_success / elapsed_time if elapsed_time > 0 else 0,
            "spread_efficiency": min(elapsed_time / self.publish_interval, 1.0) * 100
        }

    def print_statistics(self):
        """Print current publisher statistics."""
        with self.metrics_lock:
            logger.info("=== Publisher Statistics ===")
            logger.info(f"Connected: {self.connected}")
            logger.info(f"Total messages published: {self.publish_count}")
            logger.info(f"Total errors: {self.error_count}")
            logger.info(f"Last cycle duration: {self.last_publish_time:.2f}s")
            logger.info(
                f"Success rate: {(self.publish_count / (self.publish_count + self.error_count) * 100) if (self.publish_count + self.error_count) > 0 else 0:.1f}%")

    def run_publisher(self):
        """
        Main publisher loop with configurable burst or spread publishing.
        """
        mode_name = "SPREAD" if self.spread_mode else "BURST"
        logger.info(f"Starting MQTT Data Publisher ({mode_name} mode)")
        logger.info(f"Configuration:")
        logger.info(f"  - Broker: {self.broker_url}")
        logger.info(f"  - Client ID: {self.client_id}")
        logger.info(f"  - Endpoints: {self.num_endpoints}")
        logger.info(f"  - Publish interval: {self.publish_interval}s")
        logger.info(f"  - Batch size: {self.batch_size}")
        logger.info(f"  - QoS: {self.publish_qos}")
        logger.info(f"  - Publishing mode: {mode_name}")

        # Connect to broker
        if not self.connect():
            logger.error("Failed to connect to MQTT broker, exiting")
            return 1

        try:
            iteration = 0
            next_cycle_time = time.time()

            while not self.shutdown_event.is_set():
                iteration += 1
                cycle_start_time = time.time()

                # Wait until it's time for the next cycle (only for spread mode)
                if self.spread_mode and cycle_start_time < next_cycle_time:
                    wait_time = next_cycle_time - cycle_start_time
                    logger.info(f"Waiting {wait_time:.2f}s until next cycle...")
                    if self.shutdown_event.wait(timeout=wait_time):
                        logger.info("Shutdown requested during cycle wait")
                        break

                logger.info(f"=== Starting publish iteration #{iteration} ===")

                # Choose publishing method based on mode
                if self.spread_mode:
                    stats = self.publish_all_endpoints_spread()
                    # Calculate next cycle time (maintain consistent intervals)
                    next_cycle_time += self.publish_interval

                    # If we're behind schedule, catch up
                    current_time = time.time()
                    if next_cycle_time < current_time:
                        logger.warning(f"Publishing cycle took longer than interval, catching up...")
                        next_cycle_time = current_time + self.publish_interval
                else:
                    stats = self.publish_all_endpoints()
                    # Wait for the full interval in burst mode
                    logger.info(f"Waiting {self.publish_interval} seconds until next iteration...")
                    if self.shutdown_event.wait(timeout=self.publish_interval):
                        logger.info("Shutdown requested during wait period")
                        break

                # Print statistics every 10 iterations
                if iteration % 10 == 0:
                    self.print_statistics()

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
        except Exception as e:
            logger.error(f"Unexpected error in publisher loop: {e}")
            return 1
        finally:
            # Clean shutdown
            self.disconnect()
            logger.info("Publisher shutdown complete")

        return 0


def main():
    """Main entry point with argument parsing."""
    import argparse

    parser = argparse.ArgumentParser(
        description='MQTT Data Publisher with Paho Client - Publishes to 2000 endpoints',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Connection arguments
    parser.add_argument('--broker', default='mqtt://localhost:1883',
                        help='MQTT broker URL')
    parser.add_argument('--username', default='admin',
                        help='MQTT username')
    parser.add_argument('--password', default='admin',
                        help='MQTT password')
    parser.add_argument('--client-id',
                        help='MQTT client ID (auto-generated if not specified)')

    # Publisher arguments
    parser.add_argument('--endpoints', type=int, default=2000,
                        help='Number of endpoints to publish to')
    parser.add_argument('--interval', type=int, default=30,
                        help='Publish interval in seconds')
    parser.add_argument('--batch-size', type=int, default=50,
                        help='Number of endpoints per batch')
    parser.add_argument('--qos', type=int, choices=[0, 1, 2], default=1,
                        help='MQTT QoS level')
    parser.add_argument('--retain', action='store_true',
                        help='Set retain flag on published messages')

    # Publishing mode arguments
    parser.add_argument('--mode', choices=['burst', 'spread'], default='spread',
                        help='Publishing mode: burst (all at once) or spread (distributed over interval)')

    # Logging arguments
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        default='INFO', help='Logging level')
    parser.add_argument('--log-file', help='Log file path (default: mqtt_publisher.log)')

    args = parser.parse_args()

    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Create a publisher instance
    publisher = MQTTDataPublisher(
        broker_url=args.broker,
        username=args.username,
        password=args.password,
        client_id=args.client_id
    )

    # Configure publisher settings
    publisher.num_endpoints = args.endpoints
    publisher.publish_interval = args.interval
    publisher.batch_size = args.batch_size
    publisher.publish_qos = args.qos
    publisher.retain_messages = args.retain
    publisher.spread_mode = (args.mode == 'spread')

    # Run publisher
    exit_code = publisher.run_publisher()
    sys.exit(exit_code)


if __name__ == '__main__':
    main()