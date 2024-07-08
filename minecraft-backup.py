#!/usr/bin/python

import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta
import json
import logging
import os
from plyer import notification
import tempfile
import shutil
import sys

#PROGRAM_DIR = os.path.dirname(__file__)
PROGRAM_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))	
CONFIG_PATH = os.path.join(PROGRAM_DIR, "minecraft-backup.conf.json")
""" ex) minecraft-backup.conf.json
{
    "aws_access_key_id": "*******",
    "aws_secret_access_key": "******",
    "region_name": "ap-northeast-1",
    "world_data_path": "C:\\Users\\kusumoto\\AppData\\Local\\Packages\\Microsoft.MinecraftUWP_8wekyb3d8bbwe\\LocalState\\games\\com.mojang\\minecraftWorlds",
    "worlds": [
        "EvzZZWjpAgA=",
        "oMCHZhjmAAA="
    ],
    "s3_backet": "aks3-minecraft",
    "n_backup_generation": "3"
}
"""

JST = timezone(timedelta(hours=+9), 'JST')

logger = logging.getLogger("minecraft-backup")
logging.basicConfig(filename=os.path.join(PROGRAM_DIR, "minecraft-backup.log"), 
					encoding='utf-8', 
					level=logging.INFO,
					format='%(asctime)s - %(message)s', 
					datefmt='%Y/%m/%d %H:%M:%S')

def info(msg: str):
	print(msg)
	logger.info(msg)

def error(msg: str, e: Exception):
	print(f"{msg}: {e}")
	logger.error(f"{msg}: {e}")
	notification.notify(title="Backup Failed", message=msg)


def backup(s3_client: boto3.session.Session.client, s3_backet: str, world_data_path: str, target_dir: str):
	with tempfile.TemporaryDirectory() as tmp_dir:
		zip_file = target_dir + ".zip"
		zip_path_noext = os.path.join(tmp_dir, target_dir)
		zip_path = zip_path_noext + ".zip"
		shutil.make_archive(zip_path_noext, format='zip', root_dir=world_data_path, base_dir=target_dir)

		# ex) zip_path = C:\Users\kusumoto\AppData\Local\Temp\tmp2rw0gxrq\oMCHZhjmAAA=.zip
		#print(f"{zip_path} is {os.path.exists(zip_path)}")

		return upload(s3_client, s3_backet, zip_path)

def upload(s3_client: boto3.session.Session.client, s3_backet:str, zip_path: str):
	zip_file = os.path.basename(zip_path)
	zip_file_noext = os.path.splitext(zip_file)[0]
	now =  datetime.now(JST)
	upload_filename = f"{zip_file_noext}_{now:%Y%m%d%H%M%S}.zip"

	try:
		s3_client.upload_file(zip_path, s3_backet, upload_filename)
	except ClientException as e:
		error(f"Failed to backup world data {upload_filename} to S3 backet {s3_backet}",e)
		raise e

	# ex) backuped oMCHZhjmAAA=_20240706.zip on aks3-minecraft
	info(f"backuped {upload_filename} to {s3_backet}")

	return {"Key":upload_filename, "LastModified": now}

def delete_old_backup(s3_client: boto3.session.Session.client, s3_backet: str, backups, world: str, generation: int):
	#objects = s3_client.list_objects(Bucket=s3_backet, Prefix=world)
	sorted_backups = sorted([backup for backup in backups if backup['Key'].startswith(world)], key=lambda x: x['LastModified'])
	delete_backups = sorted_backups[:-generation]

	for backup in delete_backups:
		try:
			s3_client.delete_object(Bucket=s3_backet, Key=backup['Key'])
			info(f"deleted old backup {backup['Key']}")
		except ClientException as e:
			error(f"Failed to delete old backup world data {backup['Key']} on S3 backet {s3_backet}", e)

def load_config():
	with open(CONFIG_PATH, "r") as f:
		return json.load(f)

def is_backuped(backups, world_data_path :str, world: str) -> bool:		
	world_path = os.path.join(world_data_path, world)
	world_mtime_timestamp = os.path.getmtime(world_path)
	world_mtime = datetime.fromtimestamp(world_mtime_timestamp, JST)

	for backup in backups:
		if backup['Key'].startswith(world) and \
		   backup['LastModified'] >= world_mtime:
			return True
	else:
		return False

def get_backups(s3_client: boto3.session.Session.client, s3_backet: str):
	objects = s3_client.list_objects(Bucket=s3_backet)
	return [content for content in objects['Contents'] if content['Key'].endswith('.zip')]
	# ex) content
	# [{'Key': 'EvzZZWjpAgA=_20240706.zip', 'LastModified': datetime.datetime(2024, 7, 6, 14, 36, 38, tzinfo=tzutc()), 'ETag': '"518734ef407ca6d95fa6d68b06f78ba5-17"', 'Size': 135211868, 'StorageClass': 'STANDARD', 'Owner': {'DisplayName': 'gkusumoto', 'ID': '091d46f8f3cc9802f81a28debf22ba8c386962b85305c6e0091a93336e5c097d'}}]

if __name__ == '__main__':
	try:
		config = load_config()
		#print(config)
	except Exception as e:
		error(f"Failed to load config {CONFIG_PATH}", e)
		sys.exit(1)

	s3_client = boto3.client(
		's3',
		aws_access_key_id = config['aws_access_key_id'],
		aws_secret_access_key = config['aws_secret_access_key'],
		region_name = config['region_name']
	)

	try:
		backups = get_backups(s3_client, config['s3_backet'])
	except Exception as e:
		error(f"Failed to get object list from S3 bucket {config['s3_backet']}", e)
		sys.exit(1)
	
	for world in config['worlds']:
		info(f"backup {world}")
		try:
			if not is_backuped(backups, config['world_data_path'], world):
				obj = backup(s3_client, config['s3_backet'], config['world_data_path'], world)
				backups.append(obj)
			else:
				info(f"{world} is already backuped")
		
			delete_old_backup(s3_client, config['s3_backet'], backups, world, int(config['n_backup_generation']))

		except OSError as e:
			error(f"Failed to access world data {world}", e)
		except ClientError as e:
			# already output log in each function
			pass

