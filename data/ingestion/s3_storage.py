import boto3
import json
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, date
from io import StringIO, BytesIO
from loguru import logger
from config.settings import settings
import pyarrow as pa
import pyarrow.parquet as pq


class S3StorageManager:
    """Manages S3 storage operations for the zoo digital platform."""
    
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region
        )
        
        self.bucket_name = settings.s3_bucket_name
        self.raw_prefix = settings.s3_raw_prefix
        self.silver_prefix = settings.s3_silver_prefix
        self.gold_prefix = settings.s3_gold_prefix
        
        # Ensure bucket exists
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Ensure the S3 bucket exists, create if it doesn't."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"S3 bucket {self.bucket_name} exists")
        except Exception as e:
            logger.info(f"Creating S3 bucket {self.bucket_name}")
            try:
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': settings.aws_region}
                )
                logger.info(f"S3 bucket {self.bucket_name} created successfully")
            except Exception as create_error:
                logger.error(f"Failed to create S3 bucket: {create_error}")
                raise
    
    def _generate_s3_key(self, data_type: str, layer: str, timestamp: datetime, 
                        format: str = "parquet") -> str:
        """Generate S3 key with proper partitioning."""
        date_str = timestamp.strftime("%Y/%m/%d")
        hour_str = timestamp.strftime("%H")
        
        if layer == "raw":
            prefix = self.raw_prefix
        elif layer == "silver":
            prefix = self.silver_prefix
        elif layer == "gold":
            prefix = self.gold_prefix
        else:
            prefix = layer
        
        return f"{prefix}/{data_type}/year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/hour={hour_str}/data.{format}"
    
    def save_raw_data(self, data_type: str, data: List[Dict[str, Any]], 
                     timestamp: datetime = None) -> str:
        """Save raw data to S3 in Parquet format."""
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Add metadata columns
            df['_ingestion_timestamp'] = timestamp
            df['_data_type'] = data_type
            df['_source'] = 'ingestion'
            
            # Convert to Parquet
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)
            
            # Generate S3 key
            s3_key = self._generate_s3_key(data_type, "raw", timestamp, "parquet")
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream',
                Metadata={
                    'data_type': data_type,
                    'record_count': str(len(data)),
                    'ingestion_timestamp': timestamp.isoformat(),
                    'format': 'parquet',
                    'layer': 'raw'
                }
            )
            
            logger.info(f"Saved {len(data)} records to S3: s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Failed to save raw data to S3: {e}")
            raise
    
    def save_json_data(self, data_type: str, data: List[Dict[str, Any]], 
                      timestamp: datetime = None) -> str:
        """Save data to S3 in JSON format."""
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        try:
            # Add metadata
            enriched_data = {
                "metadata": {
                    "data_type": data_type,
                    "ingestion_timestamp": timestamp.isoformat(),
                    "record_count": len(data),
                    "source": "ingestion"
                },
                "data": data
            }
            
            # Convert to JSON
            json_data = json.dumps(enriched_data, default=str, indent=2)
            
            # Generate S3 key
            s3_key = self._generate_s3_key(data_type, "raw", timestamp, "json")
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_data,
                ContentType='application/json',
                Metadata={
                    'data_type': data_type,
                    'record_count': str(len(data)),
                    'ingestion_timestamp': timestamp.isoformat(),
                    'format': 'json',
                    'layer': 'raw'
                }
            )
            
            logger.info(f"Saved {len(data)} records to S3: s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Failed to save JSON data to S3: {e}")
            raise
    
    def save_silver_data(self, data_type: str, data: List[Dict[str, Any]], 
                        timestamp: datetime = None) -> str:
        """Save silver layer data to S3."""
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Add metadata columns
            df['_processed_timestamp'] = timestamp
            df['_data_type'] = data_type
            df['_layer'] = 'silver'
            
            # Convert to Parquet
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)
            
            # Generate S3 key
            s3_key = self._generate_s3_key(data_type, "silver", timestamp, "parquet")
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream',
                Metadata={
                    'data_type': data_type,
                    'record_count': str(len(data)),
                    'processed_timestamp': timestamp.isoformat(),
                    'format': 'parquet',
                    'layer': 'silver'
                }
            )
            
            logger.info(f"Saved {len(data)} records to S3 silver layer: s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Failed to save silver data to S3: {e}")
            raise
    
    def save_gold_data(self, data_type: str, data: List[Dict[str, Any]], 
                      timestamp: datetime = None) -> str:
        """Save gold layer data to S3."""
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Add metadata columns
            df['_processed_timestamp'] = timestamp
            df['_data_type'] = data_type
            df['_layer'] = 'gold'
            
            # Convert to Parquet
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)
            
            # Generate S3 key
            s3_key = self._generate_s3_key(data_type, "gold", timestamp, "parquet")
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream',
                Metadata={
                    'data_type': data_type,
                    'record_count': str(len(data)),
                    'processed_timestamp': timestamp.isoformat(),
                    'format': 'parquet',
                    'layer': 'gold'
                }
            )
            
            logger.info(f"Saved {len(data)} records to S3 gold layer: s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Failed to save gold data to S3: {e}")
            raise
    
    def read_data(self, s3_key: str, format: str = "parquet") -> pd.DataFrame:
        """Read data from S3."""
        try:
            if format == "parquet":
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
                df = pd.read_parquet(response['Body'])
            elif format == "json":
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
                data = json.loads(response['Body'].read().decode('utf-8'))
                df = pd.DataFrame(data['data'])
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            logger.info(f"Read {len(df)} records from S3: s3://{self.bucket_name}/{s3_key}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read data from S3: {e}")
            raise
    
    def list_files(self, prefix: str = None, layer: str = "raw") -> List[str]:
        """List files in S3 bucket with given prefix."""
        try:
            if prefix is None:
                if layer == "raw":
                    prefix = self.raw_prefix
                elif layer == "silver":
                    prefix = self.silver_prefix
                elif layer == "gold":
                    prefix = self.gold_prefix
                else:
                    prefix = ""
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            files = []
            if 'Contents' in response:
                files = [obj['Key'] for obj in response['Contents']]
            
            logger.info(f"Found {len(files)} files in S3 with prefix: {prefix}")
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files in S3: {e}")
            raise
    
    def delete_file(self, s3_key: str) -> bool:
        """Delete a file from S3."""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"Deleted file from S3: s3://{self.bucket_name}/{s3_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete file from S3: {e}")
            return False


class DataLakeManager:
    """Manages data lake operations across raw, silver, and gold layers."""
    
    def __init__(self):
        self.s3_manager = S3StorageManager()
    
    def save_to_raw_layer(self, data_type: str, data: List[Dict[str, Any]], 
                         format_type: str = 'json') -> str:
        """Save data to raw layer."""
        if format_type == 'parquet':
            return self.s3_manager.save_raw_data(data_type, data)
        else:
            return self.s3_manager.save_json_data(data_type, data)
    
    def save_to_silver_layer(self, data_type: str, data: List[Dict[str, Any]]) -> str:
        """Save processed data to silver layer."""
        # Add processing metadata
        processed_data = []
        for record in data:
            processed_record = record.copy()
            processed_record['_processed_at'] = datetime.utcnow().isoformat()
            processed_record['_data_quality_score'] = self._calculate_quality_score(record)
            processed_data.append(processed_record)
        
        return self.s3_manager.save_silver_data(data_type, processed_data)
    
    def save_to_gold_layer(self, data_type: str, data: List[Dict[str, Any]]) -> str:
        """Save aggregated/transformed data to gold layer."""
        # Add business logic metadata
        gold_data = []
        for record in data:
            gold_record = record.copy()
            gold_record['_business_timestamp'] = datetime.utcnow().isoformat()
            gold_record['_data_version'] = '1.0'
            gold_data.append(gold_record)
        
        return self.s3_manager.save_gold_data(data_type, gold_data)
    
    def _calculate_quality_score(self, record: Dict[str, Any]) -> float:
        """Calculate data quality score for a record."""
        score = 1.0
        
        # Check for null values
        null_count = sum(1 for v in record.values() if v is None)
        total_fields = len(record)
        if total_fields > 0:
            score -= (null_count / total_fields) * 0.3
        
        # Check for empty strings
        empty_count = sum(1 for v in record.values() if isinstance(v, str) and v.strip() == '')
        if total_fields > 0:
            score -= (empty_count / total_fields) * 0.2
        
        return max(0.0, min(1.0, score))


# Global instances
s3_storage = S3StorageManager()
data_lake = DataLakeManager() 