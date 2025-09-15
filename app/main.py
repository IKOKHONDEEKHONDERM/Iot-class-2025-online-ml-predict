# Import Quix Streams and other necessary libraries
import os
import logging
import joblib
import pandas as pd
from dotenv import load_dotenv
import json
from quixstreams import Application
from influxdb_client import InfluxDBClient, Point, WriteOptions
from sklearn.ensemble import RandomForestClassifier

# For local development, load environment variables from a .env file
load_dotenv()

# --- Kafka and Log Configuration ---
log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(level=log_level, format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "172.16.2.117:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "event-frames-model")
KAFKA_ML_TOPIC = os.getenv("KAFKA_ML_TOPIC", "taxi-demand-anomalies")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "taxi-anomaly-detector")

INFLUX_URL = "http://172.16.2.117:8086"  # os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

print("kafka broker:", KAFKA_BROKER)

# Initialize InfluxDB client
try:
    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    influx_writer = influx_client.write_api(write_options=WriteOptions(batch_size=1, flush_interval=1))
    logging.info("✅ InfluxDB client initialized successfully")
except Exception as e:
    logging.error(f"❌ Failed to initialize InfluxDB client: {e}")
    exit(1)

if not all([KAFKA_BROKER, KAFKA_INPUT_TOPIC, KAFKA_ML_TOPIC]):
    raise ValueError("Missing required environment variables for Kafka.")

# --- Resolve Model Path (ENV → fixed D:\ → uploaded → local folder) ---
def resolve_model_path() -> str:
    # 1) ENV (if provided)
    env_path = os.getenv("MODEL_PATH")
    if env_path and os.path.exists(env_path):
        return env_path

    # 2) Fixed Windows path requested by user
    win_path = r"D:\WORK\Project_Year4\miniproject\Iot-class-2025-online-ml-predict\app\rf_fd001.pkl"
    if os.path.exists(win_path):
        return win_path

    # 3) Uploaded file in this environment (if running here)
    uploaded_path = "/mnt/data/rf_fd001.pkl"
    if os.path.exists(uploaded_path):
        return uploaded_path

    # 4) Fallback: same folder as script
    script_dir = os.path.dirname(os.path.realpath(__file__))
    local_path = os.path.join(script_dir, "rf_fd001.pkl")
    if os.path.exists(local_path):
        return local_path

    # If nothing is found, return the Windows path (so the error message is clear for your machine)
    return win_path

# --- Load the Model ---
try:
    model_path = resolve_model_path()
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found at: {model_path}")
    rf_model: RandomForestClassifier = joblib.load(model_path)
    logging.info(f"✅ RandomForest model loaded successfully from: {model_path}")
except Exception as e:
    logging.error(f"❌ Failed to load the model: {e}")
    raise

# --- Initialize Quix Application ---
app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group=CONSUMER_GROUP,
    loglevel="INFO",
    state_dir=os.path.dirname(os.path.abspath(__file__)) + "/state/",
    auto_offset_reset="earliest"
)

# Define input and output topics
input_topic = app.topic(KAFKA_INPUT_TOPIC, value_deserializer="json")
# ใช้ JSON serializer ของ Quix โดยส่ง dict เข้าไปได้เลย (ไม่ต้อง encode เอง)
output_topic = app.topic(KAFKA_ML_TOPIC)

producer = app.get_producer()

# A simple buffer to store historical data for 'Lag' and 'Rolling_Mean'
data_buffer = []
BUFFER_SIZE = 20  # หั่นข้อมูล 20 จุด (ยังไม่ใช้ในโค้ดนี้)

def handle_message(row_data: dict):
    try:
        sensor_name = row_data.get("name", "")
        payload = row_data.get("payload", {}) or {}

        # Features ที่ใช้ตอน train (ตามลำดับที่โมเดลเรียนรู้)
        feature_order = ["T24","T30","T50","P15","P30","Nf","Nc","Ps30","phi","NRf","NRc","BPR","htBleed","W31","W32"]


        # เตรียม features (กัน key หาย → เติม 0.0)
        try:
            feats = [[float(payload.get(f, 0.0)) for f in feature_order]]
        except Exception as e:
            logging.error(f"❌ Feature extraction error: {e}, payload keys={list(payload.keys())}")
            return

        # Predict (RandomForestClassifier)
        try:
            prediction = int(rf_model.predict(feats)[0])
        except Exception as e:
            logging.error(f"❌ Model predict() failed: {e}")
            return

        prob = None
        if hasattr(rf_model, "predict_proba"):
            try:
                prob = float(rf_model.predict_proba(feats)[0, 1])
            except Exception as e:
                logging.warning(f"⚠️ predict_proba failed: {e}")
                prob = None

        # Timestamp
        ts = int(payload.get("timestamp", pd.Timestamp.utcnow().timestamp()*1000))
        ts_val = pd.to_datetime(ts, unit="ms", utc=True)

        # Build output payload
        result_payload = {
            "RUL_predicted": prediction,  # หรือจะใช้ชื่อ field อื่น เช่น 'class_predicted'
            "prob_warning": prob,
            "timestamp": ts,
            "date": ts_val.isoformat()
        }

        result = {
            "id": row_data.get("id", ""),
            "name": sensor_name,
            "place_id": row_data.get("place_id", ""),
            "payload": result_payload
        }

        # ส่งไป Kafka (ใช้ value_serializer="json" แล้ว จึงส่ง dict ได้เลย)
        serialized_data = json.dumps(result).encode("utf-8")
        producer.produce(
            topic=output_topic.name,
            value=serialized_data,
            timestamp=ts
        )
        logging.info(f"✅ Published to {KAFKA_ML_TOPIC} - data: {result}")

        # เขียน InfluxDB (ตัวอย่าง: เก็บค่า prediction เป็น field)
        try:
            point = (
                Point(KAFKA_ML_TOPIC)
                .tag("name", sensor_name)
                .field("RUL_predicted", prediction)
                .time(ts_val)
            )
            influx_writer.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            logging.info("[📊] Wrote prediction to InfluxDB")
        except Exception as e:
            logging.error(f"❌ Failed to write to InfluxDB: {e}")

        return result

    except Exception as e:
        logging.error(f"❌ Error processing message: {e}")

# Run the application
if __name__ == "__main__":
    sdf = app.dataframe(input_topic)
    sdf = sdf.apply(handle_message)
    app.run(sdf)
