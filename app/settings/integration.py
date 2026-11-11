# Add your integration-specific settings here
from environs import Env

env = Env()
env.read_env()

# Settings for delivering camera-trap images to TrapTagger, carried over from
# the TrapTagger dispatcher (PADAS/gundi-traptagger-dispatcher) with the same
# names and defaults.

# Image details wait in Redis until their image arrives. DB 3 and the key
# format match the dispatcher, so details cached by a dispatcher are still
# found after a destination moves to this runner.
TRAPTAGGER_REDIS_DB = env.int("TRAPTAGGER_REDIS_DB", 3)
# Seconds a connection can stay idle before it is checked with a PING on next use
REDIS_HEALTH_CHECK_INTERVAL = env.int("REDIS_HEALTH_CHECK_INTERVAL", 30)
# Retries for a Redis command that fails on a connection error
REDIS_RETRY_ATTEMPTS = env.int("REDIS_RETRY_ATTEMPTS", 3)
IMAGE_METADATA_CACHE_TTL = env.int("IMAGE_METADATA_CACHE_TTL", 3600)  # 1 hour

# Requests to one TrapTagger address, shared by every instance through Redis
MAX_REQUESTS = env.int("MAX_REQUESTS", 3)
MAX_REQUESTS_TIME_WINDOW_SEC = env.int("MAX_REQUESTS_TIME_WINDOW_SEC", 1)

# Images are read from the Gundi files bucket
BUCKET_NAME = env.str("BUCKET_NAME", "cdip-files-dev")
DELETE_FILES_AFTER_DELIVERY = env.bool("DELETE_FILES_AFTER_DELIVERY", False)

# Delivery status for the portal (ObservationDelivered / ObservationDeliveryFailed)
DISPATCHER_EVENTS_TOPIC = env.str("DISPATCHER_EVENTS_TOPIC", "dispatcher-events-dev")

# Messages older than this are dead-lettered instead of being sent
MAX_EVENT_AGE_SECONDS = env.int("MAX_EVENT_AGE_SECONDS", 86400)  # 24 hours

# Shared dead-letter topics, per data type, as the dispatchers use them
LEGACY_DEAD_LETTER_TOPIC = env.str("DEAD_LETTER_TOPIC", "dispatchers-dead-letter-prod")
OBSERVATIONS_DEAD_LETTER_TOPIC = env.str("OBSERVATIONS_DEAD_LETTER_TOPIC", "observations-dead-letter")
EVENTS_DEAD_LETTER_TOPIC = env.str("EVENTS_DEAD_LETTER_TOPIC", "events-dead-letter")
EVENTS_UPDATES_DEAD_LETTER_TOPIC = env.str("EVENTS_UPDATES_DEAD_LETTER_TOPIC", "events-updates-dead-letter")
ATTACHMENTS_DEAD_LETTER_TOPIC = env.str("ATTACHMENTS_DEAD_LETTER_TOPIC", "attachments-dead-letter")
TEXT_MESSAGES_DEAD_LETTER_TOPIC = env.str("TEXT_MESSAGES_DEAD_LETTER_TOPIC", "text-messages-dead-letter")
