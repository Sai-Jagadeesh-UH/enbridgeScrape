import io
import zipfile
from azure.storage.blob import BlobServiceClient
import os

# --- Configuration ---
CONNECTION_STRING = "<Your_Azure_Storage_Connection_String>"
CONTAINER_NAME = "<Your_Container_Name>"
SOURCE_DIRECTORY_PREFIX = "<Your_Source_Directory_Prefix>/"  # e.g., "data_to_archive/"
ZIPPED_FILE_NAME = "archive.zip"
DESTINATION_EXTRACT_PREFIX = "extracted_data/"
# ---------------------


def get_container_client(conn_str, container_name):
    """Helper function to get a container client."""
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    return blob_service_client.get_container_client(container_name)


def create_and_upload_zip(conn_str, container_name, source_prefix, zip_name):
    """
    Creates a zip file from blobs within a specific prefix in an Azure container, 
    using in-memory processing, and uploads the zip file to the same container.
    """
    container_client = get_container_client(conn_str, container_name)
    zip_buffer = io.BytesIO()

    # Use ZIP_DEFLATED for compression (default, but explicit for clarity)
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        blob_list = container_client.list_blobs(name_starts_with=source_prefix)
        found_blobs = False
        for blob in blob_list:
            found_blobs = True
            print(f"Adding {blob.name} to zip with DEFLATE compression...")
            blob_client = container_client.get_blob_client(blob.name)
            blob_data = blob_client.download_blob().readall()
            zip_file.writestr(blob.name, blob_data)

        if not found_blobs:
            print(f"No blobs found in prefix: {source_prefix}")
            return

    zip_buffer.seek(0)
    zip_blob_client = container_client.get_blob_client(zip_name)
    zip_blob_client.upload_blob(zip_buffer.getvalue(), overwrite=True)
    print(f"\nSuccessfully created and uploaded '{zip_name}'.")


def download_and_decompress_zip(conn_str, container_name, zip_name, destination_prefix):
    """
    Downloads a zip file from Azure, decompresses it in memory, and uploads 
    the extracted files back to Azure with a new destination prefix.
    """
    print(f"Starting decompression of '{zip_name}'...")
    container_client = get_container_client(conn_str, container_name)
    zip_blob_client = container_client.get_blob_client(zip_name)

    # Download the zip file content into memory
    zip_data = zip_blob_client.download_blob().readall()
    zip_buffer = io.BytesIO(zip_data)

    with zipfile.ZipFile(zip_buffer, 'r') as zip_file:
        # Iterate over all files inside the zip archive
        for file_info in zip_file.infolist():
            file_name = file_info.filename
            # Skip directories
            if file_name.endswith('/'):
                continue

            # Read the file content from the zip archive
            file_bytes = zip_file.read(file_name)

            # Define the new blob name in Azure (e.g., extracted_data/path/to/file.txt)
            new_blob_name = os.path.join(
                destination_prefix, file_name).replace("\\", "/")

            print(f"Uploading extracted file: {new_blob_name}")
            # Upload the extracted data to Azure Blob Storage
            extracted_blob_client = container_client.get_blob_client(
                new_blob_name)
            extracted_blob_client.upload_blob(file_bytes, overwrite=True)

    print(
        f"\nSuccessfully decompressed '{zip_name}' and uploaded files to '{destination_prefix}'.")


if __name__ == "__main__":
    # Example 1: Create and upload the zip file
    print("--- Creating Archive ---")
    create_and_upload_zip(CONNECTION_STRING, CONTAINER_NAME,
                          SOURCE_DIRECTORY_PREFIX, ZIPPED_FILE_NAME)

    # Example 2: Download and decompress the zip file to a different location/prefix
    print("\n--- Decompressing Archive ---")
    download_and_decompress_zip(
        CONNECTION_STRING, CONTAINER_NAME, ZIPPED_FILE_NAME, DESTINATION_EXTRACT_PREFIX)
