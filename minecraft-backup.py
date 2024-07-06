#!/usr/bin/python

import os 
import zipfile
import shutil
import boto3
import tempfile
import datetime

#BASEPATH = "/Users/akira.kusumoto/work/work/python/minecraft_backup/tmp"
BASEPATH = r"C:\Users\kusumoto\AppData\Local\Packages\Microsoft.MinecraftUWP_8wekyb3d8bbwe\LocalState\games\com.mojang\minecraftWorlds"
TARGET_DIRS = ["EvzZZWjpAgA=", "oMCHZhjmAAA="]
S3_BACKET = "aks3-minecraft"
N_BACKUP_GENERATION = 3

def backup(s3_client: boto3.session.Session.client, target_dir: str):
	with tempfile.TemporaryDirectory() as tmp_dir:
		zip_file = target_dir + ".zip"
		zip_path_noext = os.path.join(tmp_dir, target_dir)
		zip_path = zip_path_noext + ".zip"
		shutil.make_archive(zip_path_noext, format='zip', root_dir=BASEPATH, base_dir=target_dir)

		# ex) zip_path = C:\Users\kusumoto\AppData\Local\Temp\tmp2rw0gxrq\oMCHZhjmAAA=.zip
		#print(f"{zip_path} is {os.path.exists(zip_path)}")

		upload(s3_client, zip_path)

def upload(s3_client: boto3.session.Session.client, zip_path: str):
	zip_file = os.path.basename(zip_path)
	zip_file_noext = os.path.splitext(zip_file)[0]
	now =  datetime.datetime.now()
	upload_filename = f"{zip_file_noext}_{now:%Y%m%d%H%M%S}.zip"

	s3_client.upload_file(zip_path, S3_BACKET, upload_filename)
	# ex) backuped oMCHZhjmAAA=_20240706.zip on aks3-minecraft
	print(f"backuped {upload_filename} on {S3_BACKET}")

def delete_old_backup(s3_client: boto3.session.Session.client, target: str, generation: int):
	objects = s3_client.list_objects(Bucket=S3_BACKET, Prefix=target)
	backups = sorted([content for content in objects['Contents']], key=lambda x: x['LastModified'])
	# ex) content
	# [{'Key': 'EvzZZWjpAgA=_20240706.zip', 'LastModified': datetime.datetime(2024, 7, 6, 14, 36, 38, tzinfo=tzutc()), 'ETag': '"518734ef407ca6d95fa6d68b06f78ba5-17"', 'Size': 135211868, 'StorageClass': 'STANDARD', 'Owner': {'DisplayName': 'gkusumoto', 'ID': '091d46f8f3cc9802f81a28debf22ba8c386962b85305c6e0091a93336e5c097d'}}]
	#print(backups)
	delete_backups = backups[:-N_BACKUP_GENERATION]
	for backup in delete_backups:
		s3_client.delete_object(Bucket=S3_BACKET, Key=backup['Key'])
		print(f"deleted old backup {backup['Key']}")

if __name__ == '__main__':
	s3_client = boto3.client(
		's3',
		aws_access_key_id = '',
		aws_secret_access_key = '',
		region_name = 'ap-northeast-1'
	)
	
	for target_dir in TARGET_DIRS:
		#backup(s3_client, target_dir)
		delete_old_backup(s3_client, target_dir, 3)

import sys
sys.exit(0)


print(client.list_buckets())

print(client.list_objects('ak3-minecraft'))

