from pipeline.common.config import resolve_placeholders, select_section


def test_resolve_placeholders_substitutes_and_passes_through():
    env = {"AZURE_STORAGE_ACCOUNT": "wsnacct"}
    resolved = resolve_placeholders(
        "abfss://c@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/bronze", env
    )
    assert resolved == "abfss://c@wsnacct.dfs.core.windows.net/bronze"
    # non-string leaves pass through untouched
    assert resolve_placeholders(42, env) == 42


def test_select_section_resolves_only_active_mode():
    doc = {
        "mode": "local",
        "local": {"bronze_root": "pipeline/lake/bronze"},
        "azure": {"bronze_root": "abfss://c@${ACCT}.dfs.core.windows.net/bronze"},
    }
    local = select_section(doc, "local", {})
    assert local["bronze_root"] == "pipeline/lake/bronze"
    azure = select_section(doc, "azure", {"ACCT": "wsnacct"})
    assert azure["bronze_root"] == "abfss://c@wsnacct.dfs.core.windows.net/bronze"
