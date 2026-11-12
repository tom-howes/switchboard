"""Generic REST connector tests: config validation, pagination, auth. HTTP
mocked with respx; no database."""

from __future__ import annotations

import json

import httpx
import pytest
import respx
from pydantic import ValidationError

from switchboard.connectors.generic import GenericConfig, GenericRestConnector, load_configs
from switchboard.core.models import Credentials
from switchboard.engine.errors import PermanentError

BASE = "https://api.acme.example/v1"


def config(**overrides) -> dict:
    base = {
        "id": "acme",
        "base_url": BASE,
        "auth": {"style": "bearer", "env": {"api_key": "ACME_API_KEY"}},
        "list": {"path": "/contacts", "records": "$.data", "id": "$.contact_id"},
        "fetch_one": {"path": "/contacts/{id}", "record": "$.data"},
        "pagination": {"style": "cursor", "param": "cursor", "next": "$.meta.next"},
    }
    base.update(overrides)
    return base


def connector(**overrides) -> GenericRestConnector:
    return GenericRestConnector(GenericConfig.model_validate(config(**overrides)))


KEY = Credentials(api_key="secret")


class TestConfig:
    def test_a_valid_config_loads(self):
        assert GenericConfig.model_validate(config()).reconcile_every_hours == 1.0

    @pytest.mark.parametrize("bad", [
        {"base_url": "http://api.acme.example"},                       # plain http off localhost
        {"fetch_one": {"path": "/contacts"}},                          # no {id}
        {"auth": {"style": "basic", "env": {"api_key": "X"}}},          # basic needs username + password
        {"pagination": {"style": "offset", "param": "offset"}},        # unsupported style
    ])
    def test_mistakes_are_caught_when_the_file_is_loaded(self, bad):
        with pytest.raises(ValidationError):
            GenericConfig.model_validate(config(**bad))

    def test_fetch_one_is_required(self):
        cfg = config()
        del cfg["fetch_one"]
        with pytest.raises(ValidationError):
            GenericConfig.model_validate(cfg)

    def test_file_name_must_match_id(self, tmp_path):
        (tmp_path / "wrong.json").write_text(json.dumps(config()))
        with pytest.raises(ValueError, match="must match the file name"):
            load_configs(tmp_path)

    def test_shipped_example_is_valid(self):
        assert "acme_crm" in load_configs()


class TestPagination:
    @respx.mock
    async def test_cursor_walk(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if "cursor" not in request.url.params:
                return httpx.Response(200, json={"data": [{"contact_id": 1}], "meta": {"next": "abc"}})
            assert request.url.params["cursor"] == "abc"
            return httpx.Response(200, json={"data": [{"contact_id": 2}], "meta": {"next": None}})

        respx.get(f"{BASE}/contacts").mock(side_effect=handler)
        first = await connector().list_changes(KEY, None)
        assert ([r.external_id for r in first.records], first.next_cursor) == (["1"], "abc")
        last = await connector().list_changes(KEY, first.next_cursor)
        assert last.next_cursor is None

    @respx.mock
    async def test_page_walk_stops_on_a_short_page(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.params["per_page"] == "2"
            page = int(request.url.params["page"])
            data = [{"contact_id": 1}, {"contact_id": 2}] if page == 1 else [{"contact_id": 3}]
            return httpx.Response(200, json={"data": data})

        route = respx.get(f"{BASE}/contacts").mock(side_effect=handler)
        c = connector(pagination={"style": "page", "page_size": 2, "size_param": "per_page"})
        first = await c.list_changes(KEY, None)
        assert first.next_cursor == "2"
        last = await c.list_changes(KEY, first.next_cursor)
        assert last.next_cursor is None and route.call_count == 2  # no extra empty-page request

    @respx.mock
    async def test_page_walk_without_a_size_stops_on_an_empty_page(self):
        respx.get(f"{BASE}/contacts").mock(return_value=httpx.Response(200, json={"data": []}))
        page = await connector(pagination={"style": "page"}).list_changes(KEY, "4")
        assert page.records == [] and page.next_cursor is None

    @respx.mock
    async def test_a_record_without_an_id_is_an_error_not_a_guess(self):
        respx.get(f"{BASE}/contacts").mock(return_value=httpx.Response(200, json={"data": [{"name": "x"}]}))
        with pytest.raises(PermanentError, match="nothing at"):
            await connector().list_changes(KEY, None)


class TestAuth:
    @respx.mock
    @pytest.mark.parametrize("auth, check", [
        ({"style": "bearer", "env": {"api_key": "K"}},
         lambda r: r.headers["authorization"] == "Bearer secret"),
        ({"style": "header", "header": "X-Api-Key", "env": {"api_key": "K"}},
         lambda r: r.headers["x-api-key"] == "secret"),
    ])
    async def test_token_styles(self, auth, check):
        route = respx.get(f"{BASE}/contacts").mock(return_value=httpx.Response(200, json={"data": []}))
        await connector(auth=auth).list_changes(KEY, None)
        assert check(route.calls[0].request)

    @respx.mock
    async def test_basic(self):
        route = respx.get(f"{BASE}/contacts").mock(return_value=httpx.Response(200, json={"data": []}))
        c = connector(auth={"style": "basic", "env": {"username": "U", "password": "P"}})
        await c.list_changes(Credentials(username="ada", password="pw"), None)
        assert route.calls[0].request.headers["authorization"].startswith("Basic ")

    async def test_authorize_reads_the_named_env_vars(self, monkeypatch):
        monkeypatch.setenv("ACME_API_KEY", "from-env")
        assert (await connector().authorize("", "")).api_key == "from-env"
        monkeypatch.delenv("ACME_API_KEY")
        with pytest.raises(PermanentError, match="ACME_API_KEY"):
            await connector().authorize("", "")


@respx.mock
async def test_fetch_one_404_means_gone():
    respx.get(f"{BASE}/contacts/9").mock(return_value=httpx.Response(404, json={}))
    respx.get(f"{BASE}/contacts/1").mock(return_value=httpx.Response(200, json={"data": {"contact_id": 1}}))
    assert await connector().fetch_one(KEY, "9") is None
    assert (await connector().fetch_one(KEY, "1")).payload == {"contact_id": 1}
