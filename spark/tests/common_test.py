from common import ImpactScope, source_paths


def test_full_scope_uses_partitioned_glob():
    path = "s3a://nhl-bronze/play-by-play/season=*/date=*/game_*.json"
    assert source_paths(None, path, "pbp") == [path]


def test_incremental_scope_uses_manifest_objects():
    scope = ImpactScope(
        game_ids=[2026020001],
        pbp_paths=["s3a://nhl-bronze/play-by-play/season=20262027/game.json"],
        shift_paths=["s3a://nhl-bronze/shift-charts/season=20262027/game.json"],
    )
    assert source_paths(scope, "unused", "pbp") == scope.pbp_paths
    assert source_paths(scope, "unused", "shift") == scope.shift_paths
