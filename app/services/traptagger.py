from urllib.parse import urlparse

import httpx

DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)
# Image uploads keep the dispatcher's timeouts: 10 s to connect, 20 s to read.
UPLOAD_TIMEOUT = httpx.Timeout(20.0, connect=10.0)
# TrapTagger's answer, lowercased, to a request with an accepted key and no image.
KEY_ACCEPTED_MESSAGE = "no image provided"
# TrapTagger's answer, lowercased, when it can't place the camera: no site
# identifier and no usable location (for example 0,0).
MISSING_SITE_MESSAGE = "missing site information"


def add_image_url(base_url: str) -> str:
    """The upload endpoint the TrapTagger dispatcher posts images to.

    Raises ValueError when base_url isn't a full web address (scheme and host).
    """
    parsed = urlparse(base_url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError(f"'{base_url}' isn't a full web address")
    # hostname drops the brackets from IPv6 literals; put them back.
    host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
    if parsed.port:
        host = f"{host}:{parsed.port}"
    return f"{parsed.scheme}://{host}/api/v1/addImage"


async def check_api_key(base_url: str, api_key: str) -> httpx.Response:
    """Post an empty request (no image) to the upload endpoint.

    TrapTagger answers 401 "Unauthorized access." when the key is wrong or
    missing, and 400 "No image provided." when the key is accepted. No image
    is sent, so nothing is created.
    """
    url = add_image_url(base_url)
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        return await client.post(url, headers={"apikey": api_key})


async def add_image(base_url: str, api_key: str, image_metadata: dict, file_name: str, file_data: bytes) -> dict:
    """Upload one image with its details (camera, latitude, longitude, timestamp).

    Sends the same request as the TrapTagger dispatcher: a multipart form with
    the details as fields, the file as "image", and the key in the "apikey"
    header. Returns TrapTagger's JSON answer, which holds the new "image_id".
    Raises httpx.HTTPStatusError on a non-2xx answer.
    """
    url = add_image_url(base_url)
    async with httpx.AsyncClient(timeout=UPLOAD_TIMEOUT) as client:
        response = await client.post(
            url,
            data=image_metadata,
            headers={"apikey": api_key},
            files={"image": (file_name, file_data)},
        )
    response.raise_for_status()
    return response.json()


def is_missing_site_error(exception: Exception) -> bool:
    """TrapTagger rejected the image because it can't place the camera."""
    if not isinstance(exception, httpx.HTTPStatusError):
        return False
    if exception.response.status_code != 400:
        return False
    return MISSING_SITE_MESSAGE in exception.response.text.lower()


def is_permanent_client_error(exception: Exception) -> bool:
    """A 4xx answer that retrying can't fix.

    401/403 (a rotated or expired key) are retried because they're usually
    fixed in the portal; 408 (timeout) and 429 (rate limit) are transient.
    """
    if not isinstance(exception, httpx.HTTPStatusError):
        return False
    status_code = exception.response.status_code
    return 400 <= status_code < 500 and status_code not in (401, 403, 408, 429)
