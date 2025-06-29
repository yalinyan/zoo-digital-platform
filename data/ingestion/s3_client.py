import json
import boto3
import pandas as pd
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, date
from io import StringIO, BytesIO
from botocore.exceptions import ClientError, NoCredentialsError
from loguru import logger
from config.settings import settings


class S3Client:
    def __init__(self):
        self.s3_client = None
        self.s3_resource = None
        self.bucket_name = settings.s3_bucket_name
        self.raw_prefix = settings.s3_raw_prefix
        self.silver_prefix = settings.s3_silver_prefix
        self.gold_prefix = settings.s3_gold_prefix
        
        self._initialize_s3_client()
    
    def _initialize_s3_client(self):
        """Initialize S3 client with credentials."""
        try:
            session_kwargs = {
                'region_name': settings.aws_region
            }
            
            # Add credentials if provided
            if settings.aws_access_key_id and settings.aws_secret_access_key:
                session_kwargs.update({
                    'aws_access_key_id': settings.aws_access_key_id,
                    'aws_secret_access_key': settings.aws_secret_access_key
                })
            
            session = boto3.Session(**session_kwargs)
            self.s3_client = session.client('s3')
            self.s3_resource = session.resource('s3')
            
            logger.info(f"S3 client initialized for bucket: {self.bucket_name}")
            
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please configure credentials.")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise
    
    def _generate_s3_key(self, data_type: str, source: str, timestamp: datetime, 
                        format: str = 'json', partition_by: Optional[str] = None) -> str:
        """
        Generate S3 key with proper partitioning structure.
        
        Args:
            data_type: Type of data (animals, visitors, weather, etc.)
            source: Data source identifier
            timestamp: Data timestamp
            format: File format (json, parquet, csv)
            partition_by: Partition column (year, month, day, hour)
            
        Returns:
            str: S3 key path
        """
        year = timestamp.year
        month = timestamp.month
        day = timestamp.day
        hour = timestamp.hour
        
        if partition_by == 'year':
            partition_path = f"year={year}"
        elif partition_by == 'month':
            partition_path = f"year={year}/month={month:02d}"
        elif partition_by == 'day':
            partition_path = f"year={year}/month={month:02d}/day={day:02d}"
        elif partition_by == 'hour':
            partition_path = f"year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}"
        else:
            partition_path = f"year={year}/month={month:02d}/day={day:02d}"
        
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
        filename = f"{source}_{timestamp_str}.{format}"
        
        return f"{self.raw_prefix}/{data_type}/{partition_path}/{filename}"
    
    def upload_json_data(self, data: Dict[str, Any], data_type: str, source: str, 
                        timestamp: datetime, partition_by: str = 'day') -> bool:
        """
        Upload JSON data to S3 raw layer.
        
        Args:
            data: Data to upload
            data_type: Type of data
            source: Data source
            timestamp: Data timestamp
            partition_by: Partitioning strategy
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            s3_key = self._generate_s3_key(data_type, source, timestamp, 'json', partition_by)
            
            # Convert data to JSON string
            json_data = json.dumps(data, default=str, indent=2)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_data,
                ContentType='application/json',
                Metadata={
                    'data_type': data_type,
                    'source': source,
                    'timestamp': timestamp.isoformat(),
                    'partition_by': partition_by
                }
            )
            
            logger.info(f"JSON data uploaded to s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload JSON data to S3: {e}")
            return False
    
    def upload_batch_json_data(self, data_list: List[Dict[str, Any]], data_type: str, 
                             source: str, timestamp: datetime, 
                             partition_by: str = 'day') -> Dict[str, int]:
        """
        Upload batch of JSON data to S3.
        
        Args:
            data_list: List of data dictionaries
            data_type: Type of data
            source: Data source
            timestamp: Data timestamp
            partition_by: Partitioning strategy
            
        Returns:
            Dict with success and failure counts
        """
        success_count = 0
        failure_count = 0
        
        for data in data_list:
            if self.upload_json_data(data, data_type, source, timestamp, partition_by):
                success_count += 1
            else:
                failure_count += 1
        
        logger.info(f"Batch upload completed: {success_count} success, {failure_count} failures")
        return {'success': success_count, 'failure': failure_count}
    
    def upload_parquet_data(self, df: pd.DataFrame, data_type: str, source: str,
                           timestamp: datetime, partition_by: str = 'day') -> bool:
        """
        Upload DataFrame as Parquet to S3.
        
        Args:
            df: Pandas DataFrame
            data_type: Type of data
            source: Data source
            timestamp: Data timestamp
            partition_by: Partitioning strategy
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            s3_key = self._generate_s3_key(data_type, source, timestamp, 'parquet', partition_by)
            
            # Convert DataFrame to Parquet bytes
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream',
                Metadata={
                    'data_type': data_type,
                    'source': source,
                    'timestamp': timestamp.isoformat(),
                    'partition_by': partition_by,
                    'rows': str(len(df)),
                    'columns': str(len(df.columns))
                }
            )
            
            logger.info(f"Parquet data uploaded to s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload Parquet data to S3: {e}")
            return False
    
    def download_json_data(self, s3_key: str) -> Optional[Dict[str, Any]]:
        """
        Download JSON data from S3.
        
        Args:
            s3_key: S3 object key
            
        Returns:
            Dict with data or None if failed
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            json_data = response['Body'].read().decode('utf-8')
            return json.loads(json_data)
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"Object not found: s3://{self.bucket_name}/{s3_key}")
            else:
                logger.error(f"Failed to download JSON data: {e}")
            return None
        except Exception as e:
            logger.error(f"Error downloading JSON data: {e}")
            return None
    
    def download_parquet_data(self, s3_key: str) -> Optional[pd.DataFrame]:
        """
        Download Parquet data from S3 as DataFrame.
        
        Args:
            s3_key: S3 object key
            
        Returns:
            DataFrame or None if failed
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            parquet_data = response['Body'].read()
            
            # Read Parquet from bytes
            df = pd.read_parquet(BytesIO(parquet_data))
            return df
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"Object not found: s3://{self.bucket_name}/{s3_key}")
            else:
                logger.error(f"Failed to download Parquet data: {e}")
            return None
        except Exception as e:
            logger.error(f"Error downloading Parquet data: {e}")
            return None
    
    def list_objects(self, prefix: str, max_keys: int = 1000) -> List[str]:
        """
        List objects in S3 bucket with given prefix.
        
        Args:
            prefix: S3 key prefix
            max_keys: Maximum number of keys to return
            
        Returns:
            List of S3 keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []
                
        except Exception as e:
            logger.error(f"Failed to list S3 objects: {e}")
            return []
    
    def delete_object(self, s3_key: str) -> bool:
        """
        Delete object from S3.
        
        Args:
            s3_key: S3 object key
            
        Returns:
            bool: True if deletion successful, False otherwise
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"Object deleted: s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete object: {e}")
            return False
    
    def copy_object(self, source_key: str, dest_key: str, 
                   source_bucket: Optional[str] = None) -> bool:
        """
        Copy object within S3 bucket or between buckets.
        
        Args:
            source_key: Source object key
            dest_key: Destination object key
            source_bucket: Source bucket (defaults to current bucket)
            
        Returns:
            bool: True if copy successful, False otherwise
        """
        try:
            source_bucket = source_bucket or self.bucket_name
            
            copy_source = {
                'Bucket': source_bucket,
                'Key': source_key
            }
            
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=dest_key
            )
            
            logger.info(f"Object copied: s3://{source_bucket}/{source_key} -> s3://{self.bucket_name}/{dest_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to copy object: {e}")
            return False
    
    def get_object_metadata(self, s3_key: str) -> Optional[Dict[str, Any]]:
        """
        Get object metadata from S3.
        
        Args:
            s3_key: S3 object key
            
        Returns:
            Dict with metadata or None if failed
        """
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return {
                'content_length': response.get('ContentLength'),
                'last_modified': response.get('LastModified'),
                'content_type': response.get('ContentType'),
                'metadata': response.get('Metadata', {})
            }
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"Object not found: s3://{self.bucket_name}/{s3_key}")
            else:
                logger.error(f"Failed to get object metadata: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting object metadata: {e}")
            return None


# Global S3 client instance
s3_client = S3Client() 