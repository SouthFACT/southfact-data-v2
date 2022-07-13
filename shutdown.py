from __future__ import print_function
import logging, boto3, pickle, io, argparse, contextlib, json, re, threading, time, uuid, ee, subprocess, datetime, os, pathlib
from botocore.exceptions import ClientError
from os import listdir
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
from os.path import isfile, join
from pathlib import Path
from userConfig import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, downloadDir, outputGeoTIFFDir, ids_file, drive_key_file, credentials_file
  
 
def main():  
    ec2_client = boto3.client('ec2',region_name='us-east-1',aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    InstanceID = Path('/home/ec2-user/InstanceID.txt').read_text()
    response = ec2_client.describe_instances(InstanceIds=[InstanceID])
    os.remove('/home/ec2-user/InstanceID.txt')
    ec2_client.stop_instances(InstanceIds=[InstanceID])

if __name__ == '__main__':
    main()