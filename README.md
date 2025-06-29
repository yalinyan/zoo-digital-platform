# Zoo Digital Platform

A comprehensive digital platform for zoo management, featuring real-time data ingestion, processing, and analytics with AI-powered insights.

## 🏗️ Architecture Overview

The platform follows a modern data lake architecture with the following layers:

- **Raw Layer**: Raw data ingestion from multiple sources (Kafka/S3)
- **Silver Layer**: Cleaned and validated data
- **Gold Layer**: Business-ready aggregated data
- **Dashboard**: Real-time analytics and monitoring
- **GenAI**: AI-powered chatbot and insights

## 📊 Data Sources

The platform ingests data from multiple sources:

1. **Animal Data**: Health monitoring, feeding schedules, behavior tracking
2. **Visitor Data**: Ticket sales, visitor flow, satisfaction ratings
3. **Weather Data**: Environmental conditions affecting animals and visitors
4. **Sensor Data**: IoT sensors monitoring enclosures and facilities
5. **Feeding Data**: Feeding schedules, consumption tracking, nutrition management

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Apache Kafka (local or cloud)
- AWS S3 (or compatible object storage)
- Docker (optional, for local development)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd Apple
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment**
   ```bash
   cp env.example .env
   # Edit .env with your configuration
   ```

4. **Run the ingestion system**
   ```bash
   # Single ingestion
   python scripts/run_ingestion.py --data-type animals --count 10
   
   # Continuous ingestion
   python scripts/run_ingestion.py --continuous
   ```

## 📁 Project Structure

```
Apple/
├── app/
│   ├── dashboard/          # Real-time dashboard
│   └── genai/             # AI chatbot and insights
├── config/
│   └── settings.py        # Configuration management
├── data/
│   ├── ingestion/         # Data ingestion components
│   │   ├── models.py      # Data models and schemas
│   │   ├── kafka_producer.py  # Kafka producer
│   │   ├── s3_storage.py  # S3 storage manager
│   │   ├── data_generator.py  # Test data generator
│   │   └── ingestion_orchestrator.py  # Main orchestrator
│   ├── raw/              # Raw data storage
│   ├── silver/           # Cleaned data
│   └── gold/             # Business-ready data
├── scripts/
│   └── run_ingestion.py  # Ingestion runner script
├── logs/                 # Application logs
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## 🔧 Configuration

### Environment Variables

Key configuration options in `.env`:

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# S3 Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=zoo-digital-platform

# Data Sources
ZOO_API_KEY=your_zoo_api_key
WEATHER_API_KEY=your_weather_api_key
```

### Kafka Topics

The system uses the following Kafka topics:

- `zoo.animals` - Animal health and behavior data
- `zoo.visitors` - Visitor analytics and flow data
- `zoo.weather` - Environmental conditions
- `zoo.sensors` - IoT sensor readings
- `zoo.feeding` - Feeding schedules and consumption

## 📈 Data Flow

1. **Data Ingestion**: Multiple sources → Kafka → S3 Raw Layer
2. **Data Processing**: Raw → Silver (cleaned) → Gold (aggregated)
3. **Analytics**: Dashboard and GenAI consume from Gold layer
4. **Monitoring**: Real-time metrics and alerting

## 🛠️ Usage Examples

### Generate Test Data

```python
from data.ingestion.data_generator import DataGenerator

generator = DataGenerator()
animals = generator.generate_animal_data(5)
visitors = generator.generate_visitor_data(10)
```

### Run Ingestion

```python
from data.ingestion.ingestion_orchestrator import IngestionOrchestrator

orchestrator = IngestionOrchestrator()
orchestrator.register_data_source('animals', lambda: generate_animal_data())
orchestrator.start()
```

### Access S3 Data

```python
from data.ingestion.s3_storage import s3_storage

# Save data
s3_key = s3_storage.save_raw_data('animals', animal_data)

# Read data
df = s3_storage.read_data(s3_key)
```

## 🔍 Monitoring

The system includes comprehensive monitoring:

- **Prometheus Metrics**: Performance and health metrics
- **Structured Logging**: Detailed operation logs
- **Data Quality**: Validation and quality scoring
- **Error Tracking**: Comprehensive error handling

## 🧪 Testing

```bash
# Run tests
pytest tests/

# Run with coverage
pytest --cov=data tests/
```

## 📊 Data Models

### Animal Data
- Health status monitoring
- Feeding schedules
- Behavioral patterns
- Environmental preferences

### Visitor Data
- Ticket analytics
- Flow patterns
- Satisfaction metrics
- Revenue tracking

### Weather Data
- Temperature and humidity
- Wind conditions
- UV index
- Visibility

### Sensor Data
- Environmental monitoring
- Air quality
- Water conditions
- Light and noise levels

## 🤖 AI Features

The GenAI component provides:

- **Smart Chatbot**: Visitor assistance and information
- **Predictive Analytics**: Animal health and visitor flow predictions
- **Anomaly Detection**: Unusual patterns in data
- **Recommendations**: Optimization suggestions for zoo operations

## 🚀 Deployment

### Local Development

```bash
# Start Kafka (using Docker)
docker-compose up -d kafka

# Run ingestion
python scripts/run_ingestion.py --continuous
```

### Production

```bash
# Using Docker
docker build -t zoo-platform .
docker run -d --name zoo-ingestion zoo-platform

# Using Kubernetes
kubectl apply -f k8s/
```

## 📝 API Documentation

The platform includes REST APIs for:

- Data ingestion endpoints
- Analytics queries
- Configuration management
- Health monitoring

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support and questions:

- Create an issue in the repository
- Check the documentation
- Review the logs in the `logs/` directory

## 🔮 Roadmap

- [ ] Real-time streaming analytics
- [ ] Advanced AI models for animal behavior
- [ ] Mobile app for zoo staff
- [ ] Integration with external zoo management systems
- [ ] Advanced visualization dashboards
- [ ] Multi-zoo federation support
