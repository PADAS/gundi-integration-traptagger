"""Read camera-trap images from the Gundi files bucket."""
import logging

from gcloud.aio.storage import Storage

from app import settings

logger = logging.getLogger(__name__)


async def download_file(file_path: str) -> bytes:
    async with Storage() as storage:
        return await storage.download(bucket=settings.BUCKET_NAME, object_name=file_path)


async def delete_file(file_path: str):
    async with Storage() as storage:
        await storage.delete(bucket=settings.BUCKET_NAME, object_name=file_path)
    logger.debug(f"File {file_path} deleted from the bucket.")
