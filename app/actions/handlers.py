import logging

import httpx

from app.services import traptagger
from .configurations import AuthenticateConfig

logger = logging.getLogger(__name__)


async def action_auth(integration, action_config: AuthenticateConfig):
    """Check that TrapTagger accepts the API key, as a destination."""
    if not integration.base_url:
        return {"error": "The TrapTagger address is missing, so the key couldn't be checked."}
    try:
        response = await traptagger.check_api_key(
            base_url=integration.base_url,
            api_key=action_config.api_key.get_secret_value(),
        )
    except ValueError:
        return {"error": f"The key couldn't be checked because the TrapTagger address "
                         f"'{integration.base_url}' isn't a full web address, like https://traptagger.co.uk."}
    except httpx.HTTPError as e:
        logger.exception(f"TrapTagger auth check failed for integration {integration.id}: {type(e).__name__}")
        return {"error": "TrapTagger couldn't be reached to check the key. Try again in a few minutes."}

    if response.status_code in (401, 403):
        return {"valid_credentials": False,
                "message": "TrapTagger didn't accept the API key. Check that it's current and try again."}
    if response.status_code == 400 and traptagger.KEY_ACCEPTED_MESSAGE in response.text.lower():
        # TrapTagger answers 400 "No image provided." once the key is accepted.
        return {"valid_credentials": True}
    if response.status_code >= 500 or response.status_code == 429:
        logger.error(f"TrapTagger auth check got {response.status_code} for integration {integration.id}")
        return {"error": "TrapTagger couldn't check the key right now. Try again in a few minutes."}
    logger.error(f"TrapTagger auth check got unexpected {response.status_code} for integration {integration.id}")
    return {"error": f"The key couldn't be checked because TrapTagger gave an unexpected answer "
                     f"(HTTP {response.status_code}). Check that the TrapTagger address is right."}
