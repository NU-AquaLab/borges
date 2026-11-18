"""Tests for the LLM client and analyzers against a fake OpenAI-compatible server.

No network access and no API key: the server runs on localhost in a thread and
answers from a function of the prompt.
"""

import importlib.util
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pandas as pd
import pytest
import yaml
from click.testing import CliRunner

import borges.config as config_module
from borges.analyzers.llm_analyzer import (
    AS_LIST_RESPONSE_FORMAT,
    ASRelationshipAnalyzer,
    FaviconAnalyzer,
    answer_asns,
)
from borges.config import APIConfig, LLMConfig, load_config, set_config
from borges.models import ASNetwork
from borges.pipeline.runner import Pipeline
from borges.cli import cli
from borges.resources import default_config_path, llm_profile_path
from borges.utils.llm_client import (
    LLMQuotaExceeded,
    create_llm_client,
    is_quota_exhausted,
    strip_reasoning,
)

PRESETS = Path(__file__).resolve().parents[1] / "docs" / "llm-backends"


def default_answer(prompt: str):
    """Siblings for the fake PeeringDB rows; None means HTTP 400."""
    if "BROKEN" in prompt:
        return None
    if "FAVICON" in prompt:
        return "Claro telecom"
    if "AS64500" in prompt:
        # 99999 is not in the text: the validator must drop it
        return '<think>sibling?</think>{"ASs": [64500, 99999]}'
    return '{"ASs": []}'


class FakeServer:
    """Minimal /v1/chat/completions endpoint that records request bodies."""

    def __init__(self, answer=default_answer):
        self.requests = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                server.requests.append(body)
                content = body["messages"][-1]["content"]
                if isinstance(content, list):
                    content = " ".join(
                        part.get("text", "")
                        for part in content
                        if part["type"] == "text"
                    )
                reply = answer(content)
                if reply is None:
                    reply = (400, {"error": {"message": "bad request"}})
                if isinstance(reply, tuple):
                    status, error = reply
                    data = json.dumps(error).encode()
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                    return
                payload = {
                    "id": "chatcmpl-test",
                    "object": "chat.completion",
                    "created": 0,
                    "model": body["model"],
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": reply},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 10,
                        "completion_tokens": 5,
                        "total_tokens": 15,
                    },
                }
                data = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}/v1"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()


@pytest.fixture
def server():
    fake = FakeServer()
    yield fake
    fake.close()


@pytest.fixture
def use_llm(tmp_path, server):
    """Load the default config with api.llm pointed at the fake server."""
    previous = config_module._config

    def configure(**llm):
        data = yaml.safe_load(default_config_path().read_text())
        data["paths"]["base_dir"] = str(tmp_path / "data")
        data["logging"]["log_dir"] = str(tmp_path / "logs")
        data["api"] = {
            "llm": {
                "base_url": server.url,
                "model": "fake-model",
                "vision_model": "fake-vision",
                "max_retries": 0,
                "timeout": 5,
                **llm,
            }
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.safe_dump(data))
        set_config(load_config(path))

    yield configure
    config_module._config = previous


class TestLLMConfig:
    def test_openai_key_is_an_alias(self):
        api = APIConfig(openai={"api_key": "k", "model": "m"})
        assert api.llm.model == "m"
        assert api.openai is api.llm

    def test_both_keys_rejected(self):
        with pytest.raises(ValueError):
            APIConfig(openai={"model": "a"}, llm={"model": "b"})

    def test_defaults_reproduce_the_paper(self):
        llm = LLMConfig()
        assert llm.model == "gpt-4o-mini"
        assert llm.api_key is None
        assert not llm.structured_output
        assert llm.max_concurrency == 1
        assert llm.cache_dir is None


class TestStripReasoning:
    def test_plain_answer_unchanged(self):
        assert strip_reasoning(' {"ASs": []} ') == ' {"ASs": []} '

    def test_think_block_removed(self):
        assert strip_reasoning('<think>\nhmm\n</think>\n{"ASs": [1]}') == '{"ASs": [1]}'

    def test_opened_in_template(self):
        assert strip_reasoning('hmm</think>{"ASs": [1]}') == '{"ASs": [1]}'


class TestLLMClient:
    def test_local_server_needs_no_key(self, use_llm, server):
        use_llm(api_key=None)
        client = create_llm_client()
        assert client.api_key == "unused"
        assert (
            client.invoke([{"role": "user", "content": "hi"}]).content == '{"ASs": []}'
        )

    def test_request_options(self, use_llm, server):
        use_llm(
            seed=7,
            max_tokens=64,
            extra_body={"chat_template_kwargs": {"enable_thinking": False}},
        )
        client = create_llm_client()
        response = client.invoke(
            [{"role": "user", "content": "AS64500"}],
            response_format=AS_LIST_RESPONSE_FORMAT,
        )
        body = server.requests[-1]
        assert body["seed"] == 7
        # newer langchain-openai sends the OpenAI successor field
        assert body.get("max_tokens", body.get("max_completion_tokens")) == 64
        assert body["chat_template_kwargs"] == {"enable_thinking": False}
        assert body["response_format"]["json_schema"]["name"] == "as_list"
        assert response.content == '{"ASs": [64500, 99999]}'

    def test_default_request_has_no_extras(self, use_llm, server):
        use_llm()
        create_llm_client().invoke([{"role": "user", "content": "hi"}])
        body = server.requests[-1]
        for key in (
            "seed",
            "max_tokens",
            "max_completion_tokens",
            "response_format",
            "chat_template_kwargs",
        ):
            assert key not in body

    def test_cache(self, use_llm, server, tmp_path):
        use_llm(cache_dir=str(tmp_path / "cache"))
        messages = [{"role": "user", "content": "AS64500"}]
        first = create_llm_client().invoke(messages)
        client = create_llm_client()
        second = client.invoke(messages)
        assert len(server.requests) == 1
        assert second.content == first.content
        assert client.get_usage_stats()["cache_hits"] == 1

    def test_batch_keeps_order_and_errors(self, use_llm, server):
        use_llm(max_concurrency=4)
        prompts = ["AS64500", "BROKEN", "nothing"] * 5
        responses = create_llm_client().batch_invoke(
            [[{"role": "user", "content": p}] for p in prompts],
            return_exceptions=True,
        )
        assert len(responses) == len(prompts)
        for prompt, response in zip(prompts, responses):
            if prompt == "BROKEN":
                assert isinstance(response, Exception)
            elif prompt == "AS64500":
                assert response.content == '{"ASs": [64500, 99999]}'
            else:
                assert response.content == '{"ASs": []}'


PEERINGDB = pd.DataFrame(
    [
        {"asn": 64496, "notes": "We also operate AS64500", "aka": None},
        {"asn": 64497, "notes": "BROKEN AS64501", "aka": None},
        {"asn": 64498, "notes": "Upstream: AS64502", "aka": None},
        {"asn": 64499, "notes": "No numbers here", "aka": None},
    ]
)


class TestAnalyzers:
    @pytest.mark.parametrize("concurrency", [1, 4])
    def test_as_detection(self, use_llm, server, concurrency):
        use_llm(max_concurrency=concurrency, structured_output=True)
        analyzer = ASRelationshipAnalyzer()
        relationships = analyzer.analyze_dataframe(PEERINGDB)

        by_asn = {r.source_asn: r.related_asns for r in relationships}
        # 99999 was hallucinated; 64501 failed; 64502 comes from the regex
        assert by_asn == {64496: [64500], 64498: [64502]}
        # rows without an ASN reference never reach the LLM
        assert len(server.requests) == 3
        assert all("response_format" in body for body in server.requests)

    def test_failed_answer_keeps_error(self, use_llm):
        use_llm()
        relationships = ASRelationshipAnalyzer().analyze_rows(
            [(64497, "BROKEN AS64501", None)]
        )
        assert relationships[0].related_asns == []
        assert relationships[0].error

    def test_rows_match_single_calls(self, use_llm):
        use_llm(max_concurrency=4)
        analyzer = ASRelationshipAnalyzer()
        rows = [(r.asn, r.notes, r.aka) for r in PEERINGDB.itertuples()]
        batched = analyzer.analyze_rows(rows)
        single = [analyzer.analyze_as(*row) for row in rows]
        assert [r.related_asns for r in batched] == [r.related_asns for r in single]

    def test_favicons(self, use_llm, server):
        use_llm(max_concurrency=2)
        analyses = FaviconAnalyzer().analyze_favicons(
            {"h1": b"png1", "h2": b"png2", "h3": b"png3"},
            {
                "h1": ["https://a.com", "https://b.com"],
                "h2": ["https://c.com"],
                "h3": ["https://d.com", "https://e.com"],
            },
        )
        # h2 is used by one site only
        assert len(analyses) == 2
        assert all(a.company_name == "Claro telecom" for a in analyses)
        assert all(a.is_telecom for a in analyses)
        assert server.requests[0]["model"] == "fake-vision"


class TestAnswerShapes:
    """as_detection answers come as an object, a bare list or null."""

    @pytest.mark.parametrize(
        "answer, asns",
        [
            ({"ASs": [64497, 64498]}, [64497, 64498]),
            ({"ASs": []}, []),
            ({"ASs": None}, []),
            ({"siblings": [1]}, []),  # as before: no ASs key, no siblings
            ([64497, 64498], [64497, 64498]),
            ([], []),
            (None, []),
        ],
    )
    def test_accepted(self, answer, asns):
        assert answer_asns(answer) == asns

    @pytest.mark.parametrize("answer", [64497, "AS64497", 1.5])
    def test_rejected(self, answer):
        with pytest.raises(ValueError):
            answer_asns(answer)

    @pytest.mark.parametrize("reply", ["[64500, 64501]", "null", '{"ASs": null}'])
    def test_analyzer_accepts_every_shape(self, use_llm, reply):
        fake = FakeServer(lambda prompt: reply)
        try:
            use_llm(base_url=fake.url)
            relationship = ASRelationshipAnalyzer().analyze_as(
                64496, "Transit from AS64500 and AS64501.", ""
            )
        finally:
            fake.close()
        assert relationship.error is None
        # the regex adds the ASNs in the text whatever the model said
        assert sorted(relationship.related_asns) == [64500, 64501]


def workers_ai_error(code: int, message: str):
    """A Workers AI error answer: HTTP 429 with Cloudflare's error envelope."""
    return (429, {"success": False, "errors": [{"code": code, "message": message}]})


QUOTA = workers_ai_error(
    3036,
    "AiError: You have used up your daily free allocation of 10,000 neurons, "
    "please upgrade to Cloudflare's Workers Paid plan if you would like to "
    "continue usage.",
)
BUSY = workers_ai_error(3040, "Capacity temporarily exceeded, please try again.")


class QuotaServer(FakeServer):
    """Answers ``allowed`` requests, then reports the daily quota used up."""

    def __init__(self, allowed: int):
        self.allowed = allowed
        super().__init__(answer=self.answer)

    def answer(self, prompt: str):
        if len(self.requests) > self.allowed:
            return QUOTA
        return default_answer(prompt)


@pytest.fixture
def quota_server(server):
    """The default server, swapped for one that runs out after one request."""
    fake = QuotaServer(allowed=1)
    server.url = fake.url
    yield fake
    fake.close()


class _Error(Exception):
    def __init__(self, status_code, message, body=None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def _tmp_config(tmp_path, **api):
    """The default config, writing under tmp_path, with ``api`` if given."""
    data = yaml.safe_load(default_config_path().read_text())
    data["paths"]["base_dir"] = str(tmp_path / "data")
    data["logging"]["log_dir"] = str(tmp_path / "logs")
    if api:
        data["api"] = api
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


class TestLLMProfile:
    """--llm: replace api.llm with a shipped profile or a file."""

    def test_names_resolve_to_shipped_profiles(self):
        assert llm_profile_path("workers-ai") == PRESETS / "workers-ai.yaml"
        assert llm_profile_path("ollama.yaml") == PRESETS / "ollama.yaml"

    def test_unknown_name_lists_the_shipped_ones(self):
        with pytest.raises(FileNotFoundError, match="workers-ai"):
            llm_profile_path("no-such-backend")

    def test_profile_replaces_api_llm(self, tmp_path):
        path = _tmp_config(
            tmp_path,
            openai={"model": "gpt-4o-mini", "timeout": 30},
            other={"kept": True},
        )
        profile = tmp_path / "mine.yaml"
        profile.write_text("base_url: http://gpu-host:8000/v1\nmodel: qwen3-8b\n")
        config = load_config(path, llm_profile=profile)
        assert config.api.llm.model == "qwen3-8b"
        assert config.api.llm.base_url == "http://gpu-host:8000/v1"
        # nothing from the replaced section leaks in, e.g. the old timeout
        assert config.api.llm.timeout == LLMConfig().timeout

    def test_workers_ai_profile_keeps_its_timeout(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "0123abcd")
        config = load_config(_tmp_config(tmp_path), llm_profile="workers-ai")
        assert config.api.llm.timeout == 120

    def test_cli_option(self, tmp_path):
        result = CliRunner().invoke(
            cli,
            [
                "--config",
                str(_tmp_config(tmp_path)),
                "--llm",
                "nope",
                "pipeline",
                "list",
            ],
        )
        assert result.exit_code == 1
        assert "LLM profile not found: nope" in result.output


class TestCheckScript:
    """scripts/check_llm_backend.py checks the backend, not the model."""

    @pytest.fixture
    def check(self, tmp_path, monkeypatch):
        previous = config_module._config
        spec = importlib.util.spec_from_file_location(
            "check_llm_backend",
            Path(__file__).resolve().parents[1] / "scripts" / "check_llm_backend.py",
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        def run(url):
            profile = tmp_path / "profile.yaml"
            profile.write_text(f"base_url: {url}\nmodel: fake\nmax_retries: 1\n")
            config = _tmp_config(tmp_path)
            monkeypatch.setattr(
                "sys.argv", ["check", "--config", str(config), "--llm", str(profile)]
            )
            return module.main()

        yield run
        config_module._config = previous

    def test_wrong_siblings_only_warn(self, check, server, capsys):
        # the fake model misses the siblings of one row and adds one to the other
        assert check(server.url) == 0
        assert "Warning: 2 answer(s) differ" in capsys.readouterr().out

    def test_bare_list_and_null_answers(self, check, capsys):
        answers = {"AS64497": "[64497, 64498]", "AS64500": "null"}
        fake = FakeServer(
            lambda prompt: next(v for k, v in answers.items() if k in prompt)
        )
        try:
            assert check(fake.url) == 0
        finally:
            fake.close()
        out = capsys.readouterr().out
        assert "expected [64497, 64498], got [64497, 64498] OK" in out
        assert "expected [], got [] OK" in out
        assert "Warning" not in out

    def test_account_id_is_not_printed(self, check, server, capsys):
        url = server.url.replace("/v1", "/client/v4/accounts/0123abcd/ai/v1")
        check(url)
        out = capsys.readouterr().out
        assert "0123abcd" not in out
        assert "/accounts/<account id>/" in out

    def test_failed_requests_fail(self, check, capsys):
        broken = FakeServer(lambda prompt: None)
        try:
            assert check(broken.url) == 1
        finally:
            broken.close()
        assert "2 request(s) failed" in capsys.readouterr().out


class TestWorkersAI:
    @pytest.fixture
    def preset(self, tmp_path):
        """Load the default config with the Workers AI preset as api.llm."""
        previous = config_module._config

        def load():
            config = load_config(_tmp_config(tmp_path), llm_profile="workers-ai")
            set_config(config)
            return config.api.llm

        yield load
        config_module._config = previous

    def test_preset_reads_environment(self, preset, monkeypatch):
        monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "0123abcd")
        monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "test-token")
        llm = preset()
        assert llm.base_url == (
            "https://api.cloudflare.com/client/v4/accounts/0123abcd/ai/v1"
        )
        assert llm.api_key == "test-token"
        assert llm.model.startswith("@cf/")
        assert llm.cache_dir is not None
        assert create_llm_client().api_key == "test-token"

    def test_missing_account_id_is_named(self, preset, monkeypatch):
        monkeypatch.delenv("CLOUDFLARE_ACCOUNT_ID", raising=False)
        monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "test-token")
        preset()
        with pytest.raises(ValueError, match="CLOUDFLARE_ACCOUNT_ID"):
            create_llm_client()

    def test_missing_token_is_named(self, preset, monkeypatch):
        monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "0123abcd")
        monkeypatch.delenv("CLOUDFLARE_API_TOKEN", raising=False)
        preset()
        with pytest.raises(ValueError, match="CLOUDFLARE_API_TOKEN"):
            create_llm_client()

    def test_quota_errors(self):
        assert is_quota_exhausted(_Error(429, json.dumps(QUOTA[1])))
        # the OpenAI SDK leaves Cloudflare's "errors" list out of the message
        assert is_quota_exhausted(_Error(429, "Error code: 429", body=QUOTA[1]))
        assert not is_quota_exhausted(_Error(429, json.dumps(BUSY[1])))
        assert not is_quota_exhausted(_Error(400, "daily free allocation"))
        assert not is_quota_exhausted(_Error(429, "Rate limit reached for gpt-4o"))


class TestQuotaExhausted:
    def test_batch_stops_and_rerun_resumes(self, use_llm, quota_server, tmp_path):
        # A long retry_delay would make this test hang if the client waited
        use_llm(cache_dir=str(tmp_path / "cache"), retry_delay=600)
        messages = [[{"role": "user", "content": f"AS6450{i}"}] for i in range(4)]

        with pytest.raises(LLMQuotaExceeded, match="cache"):
            create_llm_client().batch_invoke(messages, return_exceptions=True)
        # one answer, one refusal, and no request after it
        assert len(quota_server.requests) == 2

        quota_server.allowed = 100  # the quota resets
        responses = create_llm_client().batch_invoke(messages)
        assert all(r is not None for r in responses)
        # the cached first answer is not requested again
        assert len(quota_server.requests) == 2 + 3

    def test_busy_is_not_quota(self, use_llm, server):
        busy = FakeServer(answer=lambda prompt: BUSY)
        server.url = busy.url
        try:
            use_llm(retry_delay=0)
            responses = create_llm_client().batch_invoke(
                [[{"role": "user", "content": "hi"}]], return_exceptions=True
            )
        finally:
            busy.close()
        assert isinstance(responses[0], Exception)
        assert not isinstance(responses[0], LLMQuotaExceeded)

    def test_analyzer_does_not_record_empty_answers(self, use_llm, quota_server):
        use_llm()
        rows = [(r.asn, r.notes, r.aka) for r in PEERINGDB.itertuples()]
        with pytest.raises(LLMQuotaExceeded):
            ASRelationshipAnalyzer().analyze_rows(rows)
        with pytest.raises(LLMQuotaExceeded):
            ASRelationshipAnalyzer().analyze_as(64496, "AS64500", None)

    def test_pipeline_stops_without_checkpoint(self, use_llm, quota_server, tmp_path):
        use_llm()
        checkpoints = tmp_path / "checkpoints"
        pipeline = Pipeline(stage_order=["as_detection"], checkpoint_dir=checkpoints)
        pipeline.context["peeringdb_df"] = PEERINGDB
        pipeline.context["as_network"] = ASNetwork()
        with pytest.raises(LLMQuotaExceeded):
            pipeline.run()
        assert not list(checkpoints.iterdir())
