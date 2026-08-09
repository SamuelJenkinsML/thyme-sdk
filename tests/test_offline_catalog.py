"""Tests for resolving and attaching the Iceberg catalog (TH-312).

The vocabulary deliberately mirrors `offline-sink/cmd/thyme-offline-sink`: an
operator configuring the sink and a user pulling training data should not have
to learn two sets of names. `THYME_ICEBERG_CATALOG` is the catalog *type*, and
its default is `rest` on both sides.
"""

import pytest

from thyme.offline_catalog import CatalogConfig, attach_sql, table_name


class TestCatalogConfig:
    def test_defaults_match_the_sink(self, monkeypatch):
        # given no environment at all
        for var in (
            "THYME_ICEBERG_CATALOG",
            "THYME_ICEBERG_URI",
            "THYME_ICEBERG_DATABASE",
        ):
            monkeypatch.delenv(var, raising=False)

        # when resolved
        cfg = CatalogConfig.from_env()

        # then it agrees with the sink's defaults -- a reader and a writer
        # disagreeing about which catalog is authoritative is a silent
        # wrong-answer failure, not an error
        assert cfg.type == "rest"
        assert cfg.uri == "http://localhost:8181"
        assert cfg.database == "thyme"

    def test_env_overrides_each_setting(self, monkeypatch):
        # given a deployed environment
        monkeypatch.setenv("THYME_ICEBERG_CATALOG", "glue")
        monkeypatch.setenv("THYME_ICEBERG_URI", "725740881666")
        monkeypatch.setenv("THYME_ICEBERG_DATABASE", "thyme_prod")

        # when resolved
        cfg = CatalogConfig.from_env()

        # then each is taken from the environment
        assert cfg.type == "glue"
        assert cfg.uri == "725740881666"
        assert cfg.database == "thyme_prod"

    def test_an_unknown_catalog_type_is_rejected_by_name(self):
        # given a typo
        cfg = CatalogConfig(type="gluu")

        # then it fails loudly, listing what is valid -- a catalog that silently
        # resolves to nothing reads as "no data" rather than "misconfigured"
        with pytest.raises(ValueError, match="gluu"):
            attach_sql(cfg)


class TestTableNaming:
    """Must stay byte-identical to `sink.TableName`.

    The sink creates the table under this name and the reader looks it up under
    this name. Disagreeing surfaces as "no data", not as an error.
    """

    @pytest.mark.parametrize(
        "entity_type,expected",
        [
            ("UserOrderStats", "user_order_stats"),
            # Consecutive capitals: a separator goes in only after a lower or a
            # digit, so a run of capitals collapses. Not h_t_t_p_stats, and not
            # http_stats either -- parity with the sink is what matters here,
            # not what a tidier convention would produce.
            ("HTTPStats", "httpstats"),
            ("Stats", "stats"),
            ("user_order_stats", "user_order_stats"),
            ("Order2Stats", "order2_stats"),
        ],
    )
    def test_matches_the_sink(self, entity_type, expected):
        assert table_name(entity_type) == expected

    def test_table_is_qualified_by_alias_and_database(self):
        cfg = CatalogConfig(database="thyme_prod")
        assert cfg.table("UserOrderStats") == "ice.thyme_prod.user_order_stats"


class TestAttachSql:
    def test_glue_attaches_by_account_id(self):
        # given the demo cluster's catalog
        cfg = CatalogConfig(type="glue", uri="725740881666", region="us-east-1")

        # when building the attach
        stmts = "\n".join(attach_sql(cfg))

        # then it uses the Glue endpoint type, verified working on AWS 2026-08-09
        assert "ENDPOINT_TYPE 'glue'" in stmts
        assert "725740881666" in stmts

    def test_rest_attaches_by_uri(self):
        # given the local/CI catalog
        cfg = CatalogConfig(type="rest", uri="http://localhost:8181")

        # when building the attach
        stmts = "\n".join(attach_sql(cfg))

        # then it points at the REST endpoint
        assert "http://localhost:8181" in stmts
        assert "ENDPOINT_TYPE 'glue'" not in stmts

    def test_the_iceberg_extension_is_loaded(self):
        # given any catalog
        stmts = "\n".join(attach_sql(CatalogConfig()))

        # then the extension is installed and loaded before use
        assert "INSTALL iceberg" in stmts
        assert "LOAD iceberg" in stmts

    def test_explicit_credentials_are_used_when_configured(self):
        # given MinIO, as the e2e runs it
        cfg = CatalogConfig(
            type="rest",
            s3_endpoint="localhost:9000",
            s3_access_key="minioadmin",
            s3_secret_key="minioadmin",
        )

        # when building the attach
        stmts = "\n".join(attach_sql(cfg))

        # then the keys are set explicitly and SSL is off for a local endpoint
        assert "minioadmin" in stmts
        assert "credential_chain" not in stmts

    def test_absent_credentials_fall_back_to_the_provider_chain(self):
        # given a deployment where credentials come from IRSA
        cfg = CatalogConfig(type="glue", uri="725740881666")

        # when building the attach
        stmts = "\n".join(attach_sql(cfg))

        # then the ambient chain is used rather than empty keys. Setting empty
        # strings would override IRSA with an anonymous identity -- the trap the
        # sink and thyme_common::object_store::build_s3_builder both document.
        assert "credential_chain" in stmts
        assert "KEY_ID ''" not in stmts


class TestAttachDefectsFoundAgainstRealInfrastructure:
    """Two bugs the string-content tests above could not have caught.

    Both were found by running `attach_sql` against the REST catalog the e2e
    stands up, and both failed loudly at connect/query time rather than
    returning wrong data — but only once something actually executed the SQL.
    """

    def test_rest_attach_sets_an_authorization_type(self):
        # DuckDB defaults AUTHORIZATION_TYPE to oauth2, which fails outright
        # against an unauthenticated catalog:
        #   "AUTHORIZATION_TYPE is 'oauth2', yet no 'secret' was provided"
        stmts = "\n".join(attach_sql(CatalogConfig(type="rest")))

        assert "AUTHORIZATION_TYPE 'none'" in stmts

    def test_authorization_type_is_configurable_for_a_secured_catalog(self):
        stmts = "\n".join(
            attach_sql(CatalogConfig(type="rest", authorization_type="oauth2"))
        )

        assert "AUTHORIZATION_TYPE 'oauth2'" in stmts

    def test_rest_attaches_the_warehouse_not_the_namespace(self):
        # DuckDB asks a REST catalog for a *warehouse* and finds namespaces
        # inside it. Attaching the namespace resolves to nothing.
        stmts = "\n".join(
            attach_sql(
                CatalogConfig(type="rest", warehouse="warehouse", database="thyme")
            )
        )

        assert "ATTACH 'warehouse'" in stmts

    def test_a_qualified_table_carries_the_attach_alias(self):
        # `thyme.user_order_stats` resolves against DuckDB's own schemas and
        # fails with 'schema "thyme" does not exist'. The alias is required.
        cfg = CatalogConfig(alias="ice", database="thyme")

        assert cfg.table("UserOrderStats") == "ice.thyme.user_order_stats"

    def test_the_alias_used_to_attach_is_the_one_used_to_qualify(self):
        # The two have to agree, which is why the alias lives on the config
        # rather than being passed loose to attach_sql.
        cfg = CatalogConfig(alias="offline")
        stmts = "\n".join(attach_sql(cfg))

        assert "AS offline " in stmts
        assert cfg.table("Stats").startswith("offline.")
