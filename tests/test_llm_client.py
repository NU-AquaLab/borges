"""Tests for the LLM client and analyzers against a fake OpenAI-compatible server.

No network access and no API key: the server runs on localhost in a thread and
answers from a function of the prompt.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pandas as pd
import pytest
import yaml

import borges.config as config_module
from borges.analyzers.llm_analyzer import (
    AS_LIST_RESPONSE_FORMAT,
    ASRelationshipAnalyzer,
    FaviconAnalyzer,
)
from borges.config import APIConfig, LLMConfig, load_config, set_config
from borges.resources import default_config_path
from borges.utils.llm_client import create_llm_client, strip_reasoning


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
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(b'{"error": {"message": "bad request"}}')
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
