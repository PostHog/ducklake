import pytest

from hoglake_bench.cli import (
    FULL_PROFILE,
    QUICK_PROFILE,
    SCENARIOS,
    _namespace_for,
    build_parser,
)


class TestParsing:
    def test_every_scenario_parses_with_defaults(self):
        parser = build_parser()
        for name in SCENARIOS:
            args = parser.parse_args([name])
            assert args.scenario == name
            assert args.url.startswith("http")
            assert args.warmup >= 0

    def test_requires_subcommand(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args([])

    def test_comma_lists(self):
        args = build_parser().parse_args(
            [
                "commit-throughput",
                "--preseed-snapshots",
                "0,100,10000",
                "--files-per-commit",
                "1,1000",
            ]
        )
        assert args.preseed_snapshots == [0, 100, 10000]
        assert args.files_per_commit == [1, 1000]

    def test_contention_writers_list(self):
        args = build_parser().parse_args(
            ["commit-contention", "--writers", "1,2,4"]
        )
        assert args.writers == [1, 2, 4]

    def test_all_quick_full_exclusive(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["all", "--quick", "--full"])

    def test_all_defaults_to_quickish(self):
        args = build_parser().parse_args(["all"])
        assert not args.full  # quick is the default profile

    def test_common_flags_everywhere(self):
        args = build_parser().parse_args(
            ["expiry-throughput", "--url", "http://x:1", "--duration", "5"]
        )
        assert args.url == "http://x:1"
        assert args.duration == 5.0


class TestProfiles:
    def test_profiles_cover_every_scenario(self):
        assert set(QUICK_PROFILE) == set(SCENARIOS)
        assert set(FULL_PROFILE) == set(SCENARIOS)

    def test_profile_overrides_only_known_args(self):
        for name, overrides in QUICK_PROFILE.items():
            ns = _namespace_for(name, build_parser().parse_args([name]), overrides)
            for k, v in overrides.items():
                assert getattr(ns, k) == v

    def test_namespace_for_carries_common_options(self):
        base = build_parser().parse_args(
            ["all", "--url", "http://elsewhere:9", "--results", "/tmp/r.jsonl"]
        )
        ns = _namespace_for("ddl-churn", base, QUICK_PROFILE["ddl-churn"])
        assert ns.url == "http://elsewhere:9"
        assert ns.results == "/tmp/r.jsonl"
        assert ns.tables == QUICK_PROFILE["ddl-churn"]["tables"]
