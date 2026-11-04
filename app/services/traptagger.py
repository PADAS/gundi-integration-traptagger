from urllib.parse import urlparse

import httpx

DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


def add_image_url(base_url: str) -> str:
    """The upload endpoint the TrapTagger dispatcher posts images to.

    Raises ValueError when base_url isn't a full web address (scheme and host).
    """
    parsed = urlparse(base_url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError(f"'{base_url}' isn't a full web address")
    host = f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname
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
