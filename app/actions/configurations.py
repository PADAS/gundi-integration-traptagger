import pydantic

from app.actions.core import AuthActionConfiguration, ExecutableActionMixin, PushActionConfiguration
from app.services.utils import FieldWithUIOptions


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    # The TrapTagger dispatcher reads this value as "api_key" from the "auth" action config.
    api_key: pydantic.SecretStr = FieldWithUIOptions(
        ...,
        format="password",
        title="API Key",
        description="TrapTagger API key used to send images to your TrapTagger account.",
    )


# Push actions receive the data routed to a TrapTagger destination. Both read
# the TrapTagger address from the destination and the key from "auth", so
# neither needs settings of its own.
class PushImageMetadataConfig(PushActionConfiguration):
    pass


class PushImagesConfig(PushActionConfiguration):
    pass
