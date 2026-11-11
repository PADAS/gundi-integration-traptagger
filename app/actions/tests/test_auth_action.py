from types import SimpleNamespace

import httpx
import pytest

from app.actions.configurations import AuthenticateConfig
from app.actions.handlers import action_auth
from app.services import traptagger

BASE_URL = "https://traptagger.co.uk"
UPLOAD_URL = "https://traptagger.co.uk/api/v1/addImage"


@pytest.fixture
def integration():
    return SimpleNamespace(id="test-integration", base_url=BASE_URL)


@pytest.fixture
def config():
    return AuthenticateConfig(api_key="test-key")


def _mock_post(mocker, status_code=None, json=None, exc=None):
    async def post(self, url, headers=None, **kwargs):
        assert url == UPLOAD_URL
        assert headers == {"apikey": "test-key"}
        assert "files" not in kwargs
        if exc:
            raise exc
        return httpx.Response(status_code, json=json, request=httpx.Request("POST", url))

    return mocker.patch("httpx.AsyncClient.post", new=post)


def test_add_image_url_drops_path_and_query():
    assert traptagger.add_image_url("https://traptagger.co.uk/some/path?x=1") == UPLOAD_URL


def test_add_image_url_keeps_port():
    assert traptagger.add_image_url("https://host.test:8443/") == "https://host.test:8443/api/v1/addImage"


@pytest.mark.parametrize("base_url, expected", [
    ("https://[2001:db8::1]/", "https://[2001:db8::1]/api/v1/addImage"),
    ("https://[2001:db8::1]:8443/x", "https://[2001:db8::1]:8443/api/v1/addImage"),
])
def test_add_image_url_keeps_ipv6_brackets(base_url, expected):
    assert traptagger.add_image_url(base_url) == expected


@pytest.mark.parametrize("base_url", ["traptagger.co.uk", "ftp://traptagger.co.uk", "https://"])
def test_add_image_url_rejects_incomplete_address(base_url):
    with pytest.raises(ValueError):
        traptagger.add_image_url(base_url)


@pytest.mark.asyncio
async def test_key_accepted_when_traptagger_asks_for_image(mocker, integration, config):
    _mock_post(mocker, 400, {"message": "No image provided."})
    assert await action_auth(integration, config) == {"valid_credentials": True}


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [401, 403])
async def test_key_rejected(mocker, integration, config, status_code):
    _mock_post(mocker, status_code, {"message": "Unauthorized access."})
    result = await action_auth(integration, config)
    assert result["valid_credentials"] is False
    assert "didn't accept the API key" in result["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [429, 500, 503])
async def test_traptagger_unavailable_is_not_a_bad_key(mocker, integration, config, status_code):
    _mock_post(mocker, status_code, {})
    result = await action_auth(integration, config)
    assert "valid_credentials" not in result
    assert "error" in result


@pytest.mark.asyncio
async def test_network_error(mocker, integration, config):
    _mock_post(mocker, exc=httpx.ConnectTimeout("timed out"))
    result = await action_auth(integration, config)
    assert "couldn't be reached" in result["error"]


@pytest.mark.asyncio
async def test_unrelated_400_json_is_not_a_valid_key(mocker, integration, config):
    _mock_post(mocker, 400, {"message": "Bad request."})
    result = await action_auth(integration, config)
    assert "valid_credentials" not in result
    assert "HTTP 400" in result["error"]


@pytest.mark.asyncio
async def test_unrelated_400_html_is_not_a_valid_key(mocker, integration, config):
    async def post(self, url, headers=None, **kwargs):
        return httpx.Response(400, text="<html><h1>400 Bad Request</h1></html>", request=httpx.Request("POST", url))

    mocker.patch("httpx.AsyncClient.post", new=post)
    result = await action_auth(integration, config)
    assert "valid_credentials" not in result
    assert "HTTP 400" in result["error"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [200, 301, 404, 405])
async def test_unexpected_answer_is_not_a_valid_key(mocker, integration, config, status_code):
    _mock_post(mocker, status_code, {})
    result = await action_auth(integration, config)
    assert "valid_credentials" not in result
    assert f"HTTP {status_code}" in result["error"]


@pytest.mark.asyncio
async def test_missing_base_url(config):
    result = await action_auth(SimpleNamespace(id="x", base_url=None), config)
    assert "valid_credentials" not in result
    assert "address is missing" in result["error"]


@pytest.mark.asyncio
async def test_incomplete_base_url(config):
    result = await action_auth(SimpleNamespace(id="x", base_url="traptagger.co.uk"), config)
    assert "valid_credentials" not in result
    assert "isn't a full web address" in result["error"]
