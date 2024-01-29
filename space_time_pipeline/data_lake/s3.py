#--------#
# Import #
#----------------------------------------------------------------------------#

import logging
import os

import boto3

from .__base import BaseDataLake

#---------#
# classes #
#----------------------------------------------------------------------------#

class S3DataLake(BaseDataLake):
    
    def __init__(
            self,
            logger: logging,
            access_key: str = os.environ['AWS_ACCESS_KEY_ID'],
            secret_key: str = os.environ['AWS_SECRET_ACCESS_KEY'],
            bucket: str = os.environ['BUCKET_RAW_DATA'],
    ) -> None:
        """Initiate the S3DataLake instance

        Parameters
        ----------
        access_key : str, optional
            access_key from AWS, 
            by default os.environ['AWS_ACCESS_KEY_ID']
        secret_key : str, optional
            secret_key from AWS,
            by default os.environ['AWS_SECRET_ACCESS_KEY']
        """
        # Set the client
        self.set_s3_client(access_key, secret_key)
        
        # Set other parameter
        self.set_logger(logger)
    
    #------------#
    # Properties #
    #------------------------------------------------------------------------#
    
    @property
    def s3_client(self) -> boto3.client:
        """Set the access_key

        Parameters
        ----------
        access_key : str
            secret key
        """
        
        return self.__s3_client
    
    #------------------------------------------------------------------------#
    
    def set_s3_client(self, access_key: str, secret_key: str) -> None:
        """Set the S3 clent

        Parameters
        ----------
        access_key : str
            access_key
        secret_key : str
            secret_key
        """
        self.__s3_client = boto3.client(
            's3',
            aws_access_key_id = access_key, 
            aws_secret_access_key = secret_key
        )
        
    #------------------------------------------------------------------------#
    
    @property
    def logger(self) -> str:
        return self.__logger
    
    #------------------------------------------------------------------------#
    
    def set_logger(self, logger: logging) -> None:
        """Set the logger

        Parameters
        ----------
        logger : str
            logger
        """
        self.__logger = logger
    
    #---------#
    # Methods #
    #------------------------------------------------------------------------#
    # Upload #
    #--------#
    
    def upload_to_data_lake(
            self, 
            s3_bucket: str,
            prefix: str,
            target_dir: str = None,
            target_file: object = None, 
    ) -> None:
        """Upload the file to data lake

        Parameters
        ----------
        s3_bucket: str
            Target S3 bucket
        prefix : str
            Prefix that need to upload.
            It will automatically add ENV_STATE prefix.
            NO `/` at boundary 
            ex. `data/raw` -> `dev/data/raw`
        target_dir : str, optional
            Target directory, it must be a flat folder with no sub dir
            by default None
        target_file : object, optional
            Target file, must be path to file, by default None

        Raises
        ------
        ValueError
            If both `target_dir` and `target_file` are not specified.
        """
        # Check if no source provided
        if (target_dir is None) & (target_file is None):
            raise ValueError("PLEASE SPECIFY target_dir and target_file")
        
        # Check if target_dir is specified
        # Loop over dir
        elif target_dir:
            
            # List all file at dir
            file_in_dir = os.listdir(target_dir)
            
            # Iterate over file
            # Also construct the target_file
            for file in file_in_dir:
                self.upload_file(
                    s3 = self.s3_client, 
                    s3_bucket = s3_bucket,
                    prefix = prefix, 
                    target_file = os.path.join(target_dir, file)
                )
        
        # Upload the single file
        elif target_file:
            self.upload_file(
                s3 = self.s3_client, 
                s3_bucket = s3_bucket, 
                prefix = prefix, 
                target_file = target_file
            )
    
    #------------------------------------------------------------------------#
    
    def upload_file(
            self, 
            s3: boto3.client, 
            s3_bucket: str,
            prefix: str, 
            target_file: str,
    ):
        """Upload file to S3

        Parameters
        ----------
        s3 : boto3.client
            S3 client
        s3_bucket: str
            Target bucket
        prefix : str
            Target prefix
        target_file : str
            Path to file
        """
        # Split the file name
        object_key = target_file.split("/")[-1]
        
        # Handel the accidentally `/` assigned by use
        if object_key[0] == "/":
            object_key = object_key[1:]
        
        # Specify the file path and desired object key (name in S3)
        # Must be prefix/prefix/prefix
        prefix_path = f'{prefix}/{object_key}'  
        
        # Upload the file to S3
        # Also remove the file
        try:
            s3.upload_file(target_file, s3_bucket, prefix_path)
            os.remove(target_file)
            self.logger.info(f"Uploaded {prefix_path} successfully!")
            
        # Raise error
        except Exception as e:
            self.logger.info(f"Error uploading file: {e}")
    
    #------------------------------------------------------------------------#
    
    def move_file(
            self,
            source_bucket: str,
            source_path: str,
            destination_bucket: str,
            destination_path: str,
            logger: logging,
    ) -> None:
        
        # Copy and delete
        # from source_bucket/source_path
        # to destination_bucket/destination_path
        try:
            
            # Copy
            self.s3_client.copy_object(
                CopySource={'Bucket': source_bucket, 'Key': source_path},
                Bucket=destination_bucket,   
                Key=destination_path
            )
            
            # Delete
            self.s3_client.delete_object(Bucket=source_bucket, Key=source_path)
            
            # Logging
            logger.info(
                f"File moved successfully : {source_path} to {destination_path}"
            )
        
        # Raise if fail
        except Exception as e:
            logger.error(f"Error moving file: {e}")
    
    #------------------------------------------------------------------------#
    
#----------------------------------------------------------------------------#
