"""Tests for thyme.config — platform Config, env overrides, credential storage.

Per-connector configuration (Postgres host, Kafka brokers, ...) is no longer
on Config. Connector classes own that and read THYME_<TYPE>_<FIELD> env vars
directly — those tests live in test_connectors.py.
"""
from thyme.config import (
    Config,
    clear_credentials,
    load_credentials,
    save_credentials,
)


class TestConfigDefaults:
    """Given no config file or env vars, defaults should match local dev setup."""

    def test_default_api_url(self):
        config = Config()
        assert config.api_url == "http://localhost:8080/api/v1/commit"

    def test_default_api_base(self):
        config = Config()
        assert config.api_base == "http://localhost:8080"

    def test_default_query_url(self):
        config = Config()
        assert config.query_url == "http://localhost:8081"

    def test_default_api_key_is_empty(self):
        config = Config()
        assert config.api_key == ""

    def test_auth_headers_empty_when_no_key(self):
        config = Config()
        assert config.auth_headers() == {}

    def test_auth_headers_with_key(self):
        config = Config(api_key="tk_test123")
        assert config.auth_headers() == {"Authorization": "Bearer tk_test123"}


class TestConfigFromYaml:
    """Given a .thyme.yaml file, Config.load() should read platform settings."""

    def test_load_from_explicit_path(self, tmp_path):
        yaml_file = tmp_path / ".thyme.yaml"
        yaml_file.write_text(
            "api_base: http://prod.example.com\n"
            "api_key: tk_from_file\n"
            "frontend_url: http://ui.example.com\n"
        )
        config = Config.load(path=yaml_file)
        assert config.api_base == "http://prod.example.com"
        assert config.api_key == "tk_from_file"
        assert config.frontend_url == "http://ui.example.com"

    def test_load_from_cwd(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        yaml_file = tmp_path / ".thyme.yaml"
        yaml_file.write_text("api_key: tk_cwd\n")
        config = Config.load()
        assert config.api_key == "tk_cwd"

    def test_missing_file_returns_defaults(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", tmp_path / "nonexistent")
        config = Config.load()
        assert config.api_url == "http://localhost:8080/api/v1/commit"


class TestConfigEnvOverrides:
    """Given environment variables, they should override file and default values."""

    def test_api_key_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_API_KEY", "tk_env")
        config = Config.load()
        assert config.api_key == "tk_env"

    def test_api_base_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_API_BASE", "http://staging.example.com")
        config = Config.load()
        assert config.api_base == "http://staging.example.com"

    def test_query_url_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_QUERY_URL", "http://qs.example.com")
        config = Config.load()
        assert config.query_url == "http://qs.example.com"

    def test_env_overrides_yaml(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        yaml_file = tmp_path / ".thyme.yaml"
        yaml_file.write_text("api_key: tk_from_file\n")
        monkeypatch.setenv("THYME_API_KEY", "tk_from_env")
        config = Config.load()
        assert config.api_key == "tk_from_env"


class TestCredentialStorage:
    """Given thyme login/logout, credentials should persist to disk."""

    def test_save_and_load(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials"
        monkeypatch.setattr("thyme.config.CREDENTIALS_DIR", tmp_path)
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)

        save_credentials("tk_stored", "http://prod.example.com")
        creds = load_credentials()
        assert creds is not None
        assert creds["api_key"] == "tk_stored"
        assert creds["api_base"] == "http://prod.example.com"

    def test_file_permissions(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials"
        monkeypatch.setattr("thyme.config.CREDENTIALS_DIR", tmp_path)
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)

        save_credentials("tk_secret", "http://example.com")
        mode = creds_file.stat().st_mode & 0o777
        assert mode == 0o600

    def test_clear_credentials(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials"
        monkeypatch.setattr("thyme.config.CREDENTIALS_DIR", tmp_path)
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)

        save_credentials("tk_stored", "http://example.com")
        assert clear_credentials() is True
        assert load_credentials() is None

    def test_clear_nonexistent(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials"
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)
        assert clear_credentials() is False

    def test_load_no_file(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "nonexistent"
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)
        assert load_credentials() is None


class TestCredentialIntegrationWithConfig:
    """Given stored credentials, Config.load() should use them as fallback."""

    def test_stored_creds_provide_api_key(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials"
        monkeypatch.setattr("thyme.config.CREDENTIALS_DIR", tmp_path)
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)
        monkeypatch.chdir(tmp_path)

        save_credentials("tk_stored", "http://prod.example.com", "http://prod.example.com")
        config = Config.load()
        assert config.api_key == "tk_stored"
        assert config.api_base == "http://prod.example.com"

    def test_env_overrides_stored_creds(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials"
        monkeypatch.setattr("thyme.config.CREDENTIALS_DIR", tmp_path)
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)
        monkeypatch.chdir(tmp_path)

        save_credentials("tk_stored", "http://prod.example.com")
        monkeypatch.setenv("THYME_API_KEY", "tk_env_wins")
        config = Config.load()
        assert config.api_key == "tk_env_wins"

    def test_yaml_overrides_stored_creds(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials"
        monkeypatch.setattr("thyme.config.CREDENTIALS_DIR", tmp_path)
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)
        monkeypatch.chdir(tmp_path)

        save_credentials("tk_stored", "http://prod.example.com")
        (tmp_path / ".thyme.yaml").write_text("api_key: tk_yaml_wins\n")
        config = Config.load()
        assert config.api_key == "tk_yaml_wins"


class TestCoerceQuoteStripping:
    """Given quoted strings, _coerce should strip surrounding quotes."""

    def test_coerce_strips_double_quotes(self):
        from thyme.config import _coerce
        assert _coerce('"https://api.example.com"') == "https://api.example.com"

    def test_coerce_strips_single_quotes(self):
        from thyme.config import _coerce
        assert _coerce("'password&with{specials'") == "password&with{specials"

    def test_coerce_preserves_unquoted_url(self):
        from thyme.config import _coerce
        assert _coerce("http://localhost:8080") == "http://localhost:8080"

    def test_coerce_preserves_inner_quotes(self):
        from thyme.config import _coerce
        assert _coerce('say "hello"') == 'say "hello"'


class TestSimpleYamlQuotedValues:
    """Given a .thyme.yaml with quoted values, _parse_simple_yaml strips them."""

    def test_simple_yaml_quoted_api_base_with_special_chars(self, tmp_path):
        from thyme.config import _parse_simple_yaml
        f = tmp_path / ".thyme.yaml"
        f.write_text('api_base: "https://api.example.com"\n')
        data = _parse_simple_yaml(f)
        assert data["api_base"] == "https://api.example.com"
