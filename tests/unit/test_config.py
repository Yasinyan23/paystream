from paystream.core.config import Settings


def test_csv_env_values_are_split(monkeypatch) -> None:
    monkeypatch.setenv("API_KEYS", "key-a, key-b ,")
    monkeypatch.setenv("WEBHOOK_ALLOWED_HOSTS", "webhook-sink,localhost")
    settings = Settings(_env_file=None)
    assert [k.get_secret_value() for k in settings.api_keys] == ["key-a", "key-b"]
    assert settings.webhook_allowed_hosts == ["webhook-sink", "localhost"]


def test_no_api_key_by_default(monkeypatch) -> None:
    monkeypatch.delenv("API_KEYS", raising=False)
    assert Settings(_env_file=None).api_keys == []


def test_tracing_is_opt_in_and_set_by_the_otel_env_vars(monkeypatch) -> None:
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    assert Settings(_env_file=None).otel_enabled is False
    monkeypatch.setenv("OTEL_ENABLED", "true")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://jaeger:4318/v1/traces")
    settings = Settings(_env_file=None)
    assert settings.otel_enabled is True
    assert settings.otel_exporter_otlp_traces_endpoint == "http://jaeger:4318/v1/traces"


def test_retry_delays_are_exponential() -> None:
    settings = Settings(_env_file=None, retry_max_attempts=3, retry_base_delay_ms=2000)
    assert settings.retry_delays_ms == [2000, 4000]


def test_single_attempt_has_no_retry_delays() -> None:
    assert Settings(_env_file=None, retry_max_attempts=1).retry_delays_ms == []
