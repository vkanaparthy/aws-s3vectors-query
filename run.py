from S3VectorsQuery import S3VectorsQuery
# Sample product data
products = [
    {
        "id": "prod-001",
        "text": "Wireless Bluetooth headphones with noise cancellation and 30-hour battery life",
        "metadata": {"category": "electronics", "price_range": "high", "brand": "TechCorp"}
    },
    {
        "id": "prod-002", 
        "text": "Organic cotton t-shirt in multiple colors, comfortable and sustainable",
        "metadata": {"category": "clothing", "price_range": "medium", "brand": "EcoWear"}
    },
    {
        "id": "prod-003",
        "text": "Smart fitness tracker with heart rate monitor and GPS tracking",
        "metadata": {"category": "electronics", "price_range": "medium", "brand": "FitTech"}
    },
    {
        "id": "prod-004",
        "text": "Premium leather wallet with RFID blocking technology",
        "metadata": {"category": "accessories", "price_range": "high", "brand": "LuxLeather"}
    },
    {
        "id": "prod-005",
        "text": "Portable phone charger with fast charging and compact design",
        "metadata": {"category": "electronics", "price_range": "low", "brand": "PowerUp"}
    }
]

if __name__ == "__main__":
    s3vectors_query = S3VectorsQuery(bucket_name='s3vectors-query-bucket', region_name='us-east-1')
    s3vectors_query.insert_vectors(products)
   