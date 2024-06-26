##########
# Import #
##############################################################################

import logging
import os

import boto3

from .__base import BaseDataLake

###########
# classes #
##############################################################################

class DOSpacesDataLake(BaseDataLake):
    
    def __init__(
            self,
            logger: logging,
            endpoint_url: str = os.environ.get('SOS_ENDPOINT_URL'),
            access_key: str = os.environ.get('ACCESS_KEY_ID'),
            secret_key: str = os.environ.get('SECRET_ACCESS_KEY'),
    ) -> None:
        """Initiate the DOSpacesDataLake instance

        Parameters
        ----------
        access_key : str, optional
            access_key from AWS, 
            by default os.environ.get(ACCESS_KEY_ID')
        secret_key : str, optional
            secret_key from AWS,
            by default os.environ.get(SECRET_ACCESS_KEY')
        """
        # Set the client
        self.set_spaces_client(access_key, secret_key, endpoint_url)
        
        # Set other parameter
        self.set_logger(logger)
    
    ##############
    # Properties #
    ##########################################################################
    
    @property
    def spaces_client(self) -> boto3.client:
        """Set the access_key

        Parameters
        ----------
        access_key : str
            secret key
        """
        
        return self.__spaces_client
    
    ##########################################################################
    
    def set_spaces_client(
            self, 
            access_key: str, 
            secret_key: str, 
            endpoint_url: str
    ) -> None:
        """Set the spaces clent

        Parameters
        ----------
        access_key : str
            access_key
        secret_key : str
            secret_key
        """
        self.__spaces_client = boto3.client(
            's3',
            region_name='singp1',
            endpoint_url = endpoint_url,
            aws_access_key_id = access_key, 
            aws_secret_access_key = secret_key
        )
        
    ##########################################################################
    
    @property
    def logger(self) -> logging.Logger:
        return self.__logger
    
    ##########################################################################
    
    def set_logger(self, logger: logging) -> None:
        """Set the logger

        Parameters
        ----------
        logger : str
            logger
        """
        self.__logger = logger
    
    ###########
    # Methods #
    ##########################################################################
    # Upload #
    ##########
    
    def upload_to_data_lake(
            self, 
            spaces_bucket: str,
            prefix: str,
            target_dir: str = None,
            target_file: object = None, 
    ) -> None:
        """Upload the file to data lake

        Parameters
        ----------
        spaces_bucket: str
            Target spaces bucket
        prefix : str
            Prefix that need to upload.
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
            
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
            
            # List all file at dir
            file_in_dir = os.listdir(target_dir)
            
            # Iterate over file
            # Also construct the target_file
            for file in file_in_dir:
                self.upload_file(
                    spaces = self.spaces_client, 
                    spaces_bucket = spaces_bucket,
                    prefix = prefix, 
                    target_file = os.path.join(target_dir, file)
                )
        
        # Upload the single file
        elif target_file:
            self.upload_file(
                spaces = self.spaces_client, 
                spaces_bucket = spaces_bucket, 
                prefix = prefix, 
                target_file = target_file
            )
    
    ##########################################################################
    
    def upload_file(
            self, 
            spaces: boto3.client, 
            spaces_bucket: str,
            prefix: str, 
            target_file: str,
    ):
        """Upload file to spaces

        Parameters
        ----------
        spaces : boto3.client
            spaces client
        spaces_bucket: str
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
        
        # Specify the file path and desired object key (name in spaces)
        # Must be prefix/prefix/prefix
        prefix_path = f'{prefix}/{object_key}'  
        
        # Upload the file to spaces
        # Also remove the file
        try:
            spaces.upload_file(target_file, spaces_bucket, prefix_path)
            os.remove(target_file)
            self.logger.info(f"Uploaded {prefix_path} successfully!")
            
        # Raise error
        except Exception as e:
            self.logger.error(f"Error uploading file: {e}")
            raise SystemError(f"Error uploading file: {e}")
    
    ##########################################################################
    
    def move_file(
            self,
            source_bucket: str,
            source_path: str,
            destination_bucket: str,
            destination_path: str,
            logger: logging,
    ) -> None:
        """Move file from `source_bucket/source_path` to
        `destination_bucket/destination_path`

        Parameters
        ----------
        source_bucket : str
            Name of source bucket
        source_path : str
            Absolute path from bucket to file
        destination_bucket : str
            Name of destination bucket
        destination_path : str
            Absolute path from bucket to file
        logger : logging
            Log object
        """
        # Copy and delete
        # from source_bucket/source_path
        # to destination_bucket/destination_path
        try:
            # List objects in the source prefix
            response = self.spaces_client.list_objects_v2(
                Bucket=source_bucket, 
                Prefix=source_path,
            )
            
            # Copy
            # Iterate through objects and move them
            for obj in response.get('Contents', []):
                source_key = obj['Key']
                destination_key = source_key.replace(
                    source_path, 
                    destination_path, 
                    1,
                )

                # Copy the object to the new destination
                copy_source = {
                    'Bucket': destination_bucket,
                    'Key': source_key
                }
                self.spaces_client.copy_object(
                    CopySource=copy_source, 
                    Bucket=destination_bucket, 
                    Key=destination_key,
                )
                
                # Delete the original object
                self.spaces_client.delete_object(
                    Bucket=source_bucket, 
                    Key=source_key,
                )
            
            # Logging
            logger.info(
                "File moved successfully : " \
                    + f"{source_path} to {destination_path}"
            )
        
        # Raise if fail
        except Exception as e:
            logger.error(f"Error moving file: {e}")
    
    ##########################################################################
    
    def download_file(
            self,
            bucket_name: str,
            target_prefix: str, 
            logger: logging,
            local_path: str = "tmp_download",
    ) -> None:
        """Download file to local

        Parameters
        ----------
        bucket_name : str
            Name of bucket
        target_prefix : str
            Prefix of files
        logger: logging
            Logger object
        local_path : str
            Path to down load
        """
        if not os.path.exists(local_path):
            os.makedirs(local_path)
            
        # Iterate over file
        try:
            
            # List objects with the specified prefix
            response = self.spaces_client.list_objects_v2(
                Bucket=bucket_name, 
                Prefix=target_prefix,
            )

            # Download each object
            for obj in response.get('Contents', []):
                key = obj['Key']
                local_file_path = os.path.join(
                    local_path, 
                    os.path.basename(key),
                )
                
                # Ensure the local_file_path is not a directory
                if not os.path.isdir(local_file_path):
                    
                    # Download
                    self.spaces_client.download_file(
                        bucket_name, 
                        key, 
                        local_file_path,
                    )
                    
                    # Logging
                    logger.info(f"Downloaded: {key} -> {local_file_path}")
            
        # Except, error
        except Exception as e:
            print(f"Error downloading files from spaces: {e}")
            logger.info(f"Error downloading {key}: {e}")
    
    ##########################################################################
    
##############################################################################
