#!/usr/bin/env python3
"""
AWS Credentials Setup Helper
Helps you set up AWS credentials for S3 access
"""

import os
import sys
import getpass
from pathlib import Path

def create_env_file():
    """Create .env file with AWS credentials"""
    print("🔐 AWS Credentials Setup")
    print("=" * 30)
    print("You need AWS credentials to access S3 data.")
    print("If you don't have credentials, get them from AWS Console:")
    print("1. Go to AWS Console -> IAM -> Users")
    print("2. Create user or select existing user")
    print("3. Attach policy: AmazonS3ReadOnlyAccess")
    print("4. Create Access Key")
    print()
    
    # Get credentials
    access_key = input("Enter AWS Access Key ID: ").strip()
    secret_key = getpass.getpass("Enter AWS Secret Access Key: ").strip()
    region = input("Enter AWS Region (default: us-east-1): ").strip() or "us-east-1"
    bucket = input("Enter S3 Bucket Name (default: dsm_data): ").strip() or "dsm_data"
    
    # Create .env content
    env_content = f"""# AWS Configuration
AWS_ACCESS_KEY_ID={access_key}
AWS_SECRET_ACCESS_KEY={secret_key}
AWS_REGION={region}
AWS_S3_BUCKET={bucket}

# Optional settings
DEBUG=false
LOG_LEVEL=INFO
"""
    
    # Write .env file
    env_file = Path(".env")
    try:
        with open(env_file, 'w') as f:
            f.write(env_content)
        print(f"\n✅ Credentials saved to {env_file.absolute()}")
        return True
    except Exception as e:
        print(f"❌ Error saving credentials: {e}")
        return False

def test_credentials():
    """Test AWS credentials"""
    print("\n🔍 Testing AWS credentials...")
    
    try:
        import boto3
        
        # Try to create S3 client
        s3_client = boto3.client('s3')
        
        # Try to list buckets
        response = s3_client.list_buckets()
        print("✅ AWS credentials are working!")
        print(f"📦 Available buckets: {len(response['Buckets'])}")
        
        # Check if our bucket exists
        bucket_name = os.getenv('AWS_S3_BUCKET', 'dsm_data')
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"✅ Target bucket '{bucket_name}' is accessible!")
            return True
        except Exception as e:
            print(f"❌ Cannot access bucket '{bucket_name}': {e}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing credentials: {e}")
        return False

def main():
    """Main function"""
    print("🔧 AWS Credentials Setup Helper")
    print("=" * 40)
    
    # Check if .env exists
    env_file = Path(".env")
    if env_file.exists():
        print("✅ Found existing .env file")
        choice = input("Do you want to update credentials? (y/n): ").strip().lower()
        if choice != 'y':
            print("Using existing credentials...")
        else:
            if not create_env_file():
                return
    else:
        print("❌ No .env file found")
        if not create_env_file():
            return
    
    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("✅ Environment variables loaded")
    except ImportError:
        print("⚠️ python-dotenv not installed, loading .env manually")
        # Manual .env loading
        with open('.env', 'r') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
    
    # Test credentials
    if test_credentials():
        print("\n🎉 SUCCESS! You're ready to download actual data!")
        print("\nNext steps:")
        print("1. Run: python get_actual_data_with_mapping.py")
        print("2. This will download actual data and create comprehensive mapping")
    else:
        print("\n❌ Credentials test failed")
        print("\nTroubleshooting:")
        print("1. Check your AWS credentials")
        print("2. Verify S3 bucket name and permissions")
        print("3. Ensure user has 's3:ListBucket' and 's3:GetObject' permissions")

if __name__ == "__main__":
    main()
