import boto3
import numpy as np
import json
import time
from typing import List, Dict, Optional

BEDROCK_REGION = "us-east-1"
EMBEDDING_MODEL = "amazon.titan-embed-text-v2:0"
INDEX_NAME = "product-embeddings"
VECTOR_BUCKET_NAME = "s3vectors-query-bucket"

class S3VectorsQuery:
    def __init__(self, bucket_name: str = None, region_name: str = 'us-east-1'):             
        self.region_name = region_name
        self.bucket_name = bucket_name or VECTOR_BUCKET_NAME
        self.s3vectors = boto3.client('s3vectors', region_name=region_name)
        self.bedrock = boto3.client('bedrock-runtime', region_name=region_name)
        self.__create_s3vectors_bucket__()        
        self.__create_vector_index__()
    
    def __create_s3vectors_bucket__(self):
        try:
            bucket_response = self.s3vectors.create_vector_bucket(
                vectorBucketName=self.bucket_name
            )
            print(f"Created vector bucket: {self.bucket_name}")
            print(f"Bucket ARN: {bucket_response['vectorBucketArn']}")
        except Exception as e:
            if "BucketAlreadyExists" in str(e):
                print(f"Vector bucket {self.bucket_name} already exists")
            else:
                print(f"Error creating bucket: {e}")

    def __create_vector_index__(self):
        # Create vector index
        try:
            index_response = self.s3vectors.create_index(
                vectorBucketName=self.bucket_name,
                indexName=INDEX_NAME,
                dimension=1024,  # Titan embedding dimensions
                dataType="float32",
                distanceMetric="cosine"
            )
            print(f"Created vector index: {INDEX_NAME}")
            print(f"Index ARN: {index_response['indexArn']}")
        except Exception as e:
            if "IndexAlreadyExists" in str(e):
                print(f"Vector index {INDEX_NAME} already exists")
            else:
                print(f"Error creating index: {e}")

    def insert_vectors(self, products: List[Dict]):
        vectors_to_insert = []
        # Get embeddings for each product
        for product in products:
            embedding = self.get_embedding(product['text'])
            product['embedding'] = embedding
            print(f"Product: {product['id']}, {product['embedding']}, {product['metadata']}")
            vector_data = {
                "key": product['id'],
                "data": {"float32": product['embedding']},
                "metadata": product['metadata'],
            }
            vectors_to_insert.append(vector_data)
        return 
        # Insert vectors
        try:
            put_response = self.s3vectors.put_vectors(
                vectorBucketName=self.bucket_name,
                indexName=INDEX_NAME,
                vectors=vectors_to_insert
            )
            print(f"Successfully inserted {len(vectors_to_insert)} vectors")
            print(f"Request ID: {put_response['ResponseMetadata']['RequestId']}")
        except Exception as e:
            print(f"Error inserting vectors: {e}")

    def get_embedding(self, text: str) -> List[float]:
        """Get embedding for text using Titan model"""
        response = self.bedrock.invoke_model(
            modelId=EMBEDDING_MODEL,
            body=json.dumps({"inputText": text}),
            contentType="application/json"
        )
        result = json.loads(response['body'].read())
        return result['embedding']
    
    def get_vectors_by_ids(self, ids: List[str]) -> None:
        # Get vectors by ids
        try:            
            get_response = self.s3vectors.get_vectors(
                vectorBucketName=self.bucket_name,
                indexName=INDEX_NAME,
                keys=ids
            )
            print(f"Retrieved {len(get_response['vectors'])} vectors:")
            for vector in get_response['vectors']:
                print(f"- {vector['key']}")
        except Exception as e:
            print(f"Error getting vectors by ids: {e}")            

    def get_list_of_vectors(self, max_results: int = 10) -> None:
        # Get list of vectors
        try:
            list_response = self.s3vectors.list_vectors(
                vectorBucketName=self.bucket_name,
                indexName=INDEX_NAME,
                maxResults=max_results
            )
            print(f"Found {len(list_response['vectors'])} vectors in index:")
            for vector_id in list_response['vectors']:
                print(f"- {vector_id}")
                    
        except Exception as e:
            print(f"Error getting list of vectors: {e}")

    def update_vector_by_id(self, id: str, update_text: str, update_metadata: Dict = None) -> None:
        # Update vector by id
        try:
            updated_embedding = self.get_embedding(update_text)
            updated_vector = {
                "key": id,
                "data": {"float32": updated_embedding},
                "metadata": update_metadata
            }
            update_response = self.s3vectors.put_vectors(
                vectorBucketName=self.bucket_name,
                indexName=INDEX_NAME,               
                vectors=[updated_vector]
            )
            print(f"Updated vector: {id}, {updated_vector['metadata']}")
        except Exception as e:
            print(f"Error updating vector by id: {e}")

    def query(self, query: str, top_k: int = 3, filters: Optional[Dict] = None) -> List[Dict]:
        # Get embedding for query
        query_embedding = self.get_embedding(query)
        
        # Prepare query parameters
        query_params = {
            "vectorBucketName": self.bucket_name,
            "indexName": INDEX_NAME,
            "queryVector": {'float32': query_embedding},
            "topK": top_k,
            "returnMetadata": True,
            "returnDistance": True
        }
        
        # Add filters if provided
        if filters:
            query_params["filter"] = filters

        # Execute query using s3vectors.query_vectors
        try:
            response = self.s3vectors.query_vectors(**query_params)
            return response['vectors']
        except Exception as e:
            print(f"Error querying vectors: {e}")
            return []

    def query_advanced_filtering(self, query: str, top_k: int = 3, filters: Optional[Dict] = None) -> None:
        print("=== Advanced Filtering ===")
        # Filter 1: Multiple categories
        print("\n1. Electronics OR Accessories:")

        multi_category_filter = {
            "$or": [
                {"category": "electronics"},
                {"category": "accessories"}
            ]
        }
        results = self.query("premium product", top_k=5, filters=multi_category_filter)
        for result in results:
            #print(result)
            metadata = result['metadata']
            print(f"   {result['key']}: {metadata['category']} - {metadata['brand']}")

        #Filter 2: Price range exclusion
        print("\n2. Not low price range:")
        not_low_price_filter = {
            "price_range": {"$ne": "low"}
        }
        results = self.query("quality product", top_k=5, filters=not_low_price_filter)
        for result in results:
            metadata = result['metadata']
            print(f"   {result['key']}: {metadata['price_range']} - {metadata['brand']}")

        # # Filter 3: Complex AND/OR combination
        print("\n3. High-end electronics OR any TechCorp product:")
        complex_filter = {
            "$or": [
                {
                    "$and": [
                        {"category": "electronics"},
                        {"price_range": "high"}
                    ]
                },
                {"brand": "TechCorp"}
            ]
        }
        results = self.query("technology device", top_k=5, filters=complex_filter)
        for result in results:
            metadata = result['metadata']
            print(f"   {result['key']}: {metadata['category']} - {metadata['price_range']} - {metadata['brand']}")

