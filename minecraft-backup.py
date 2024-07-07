#!/usr/bin/python

import os 
import shutil
import boto3
import tempfile
from datetime import datetime, timezone, timedelta
import json
import logging

PROGRAM_DIR = os.path.dirname(__file__)
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


def backup(s3_client: boto3.session.Session.client, s3_backet: str, world_data_path: str, target_dir: str):
	with tempfile.TemporaryDirectory() as tmp_dir:
		zip_file = target_dir + ".zip"
		zip_path_noext = os.path.join(tmp_dir, target_dir)
		zip_path = zip_path_noext + ".zip"
		shutil.make_archive(zip_path_noext, format='zip', root_dir=world_data_path, base_dir=target_dir)

		# ex) zip_path = C:\Users\kusumoto\AppData\Local\Temp\tmp2rw0gxrq\oMCHZhjmAAA=.zip
		#print(f"{zip_path} is {os.path.exists(zip_path)}")

		upload(s3_client, s3_backet, zip_path)

def upload(s3_client: boto3.session.Session.client, s3_backet:str, zip_path: str):
	zip_file = os.path.basename(zip_path)
	zip_file_noext = os.path.splitext(zip_file)[0]
	now =  datetime.datetime.now()
	upload_filename = f"{zip_file_noext}_{now:%Y%m%d%H%M%S}.zip"

	s3_client.upload_file(zip_path, s3_backet, upload_filename)
	# ex) backuped oMCHZhjmAAA=_20240706.zip on aks3-minecraft
	print(f"backuped {upload_filename} on {s3_backet}")
	logger.info(f"backuped {upload_filename} on {s3_backet}")

def delete_old_backup(s3_client: boto3.session.Session.client, s3_backet: str, backups, world: str, generation: int):
	#objects = s3_client.list_objects(Bucket=s3_backet, Prefix=world)
	sorted_backups = sorted([backup for backup in backups if backup['Key'].startswith(world)], key=lambda x: x['LastModified'])
	delete_backups = sorted_backups[:-generation]
	for backup in delete_backups:
		s3_client.delete_object(Bucket=s3_backet, Key=backup['Key'])
		print(f"deleted old backup {backup['Key']}")
		logger.info(f"deleted old backup {backup['Key']}")

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
	config = load_config()
	#print(config)

	s3_client = boto3.client(
		's3',
		aws_access_key_id = config['aws_access_key_id'],
		aws_secret_access_key = config['aws_secret_access_key'],
		region_name = config['region_name']
	)

	backups = get_backups(s3_client, config['s3_backet'])
	
	for world in config['worlds']:
		if not is_backuped(backups, config['world_data_path'], world):
			print(f"backup {world}")
			logger.info(f"backup {world}")
			backup(s3_client, config['s3_backet'], config['world_data_path'], world)
		else:
			print(f"{world} is already backuped")
			logger.info(f"{world} is already backuped")
		
		delete_old_backup(s3_client, config['s3_backet'], backups, world, int(config['n_backup_generation']))
