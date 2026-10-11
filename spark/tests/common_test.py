from common import ImpactScope, impact_scope_from_payload, source_paths


def test_full_scope_uses_partitioned_glob():
    path = "s3a://nhl-bronze/play-by-play/season=*/date=*/game_*.json"
    assert source_paths(None, path, "pbp") == [path]


def test_incremental_scope_uses_manifest_objects():
    scope = ImpactScope(
        pbp_game_ids=[2026020001],
        shift_game_ids=[2026020001],
        pbp_paths=["s3a://nhl-bronze/play-by-play/season=20262027/game.json"],
        shift_paths=["s3a://nhl-bronze/shift-charts/season=20262027/game.json"],
    )
    assert source_paths(scope, "unused", "pbp") == scope.pbp_paths
    assert source_paths(scope, "unused", "shift") == scope.shift_paths


def test_impact_scope_uses_only_successfully_refreshed_games():
    payload = {
        "objects": [
            "schedule/date=2026-10-10/schedule.json",
            "play-by-play/season=20262027/date=2026-10-10/game_2026020001.json",
            "shift-charts/season=20262027/date=2026-10-10/game_2026020001.json",
        ],
        "games": [
            {
                "game_id": 2026020001,
                "pbp_status": "refreshed",
                "shift_status": "refreshed",
            },
            {
                "game_id": 2026020002,
                "pbp_status": "failed",
                "shift_status": "failed",
            },
        ],
    }

    scope = impact_scope_from_payload(payload)

    assert scope.pbp_game_ids == [2026020001]
    assert scope.shift_game_ids == [2026020001]
    assert scope.pbp_paths == [
        "s3a://nhl-bronze/play-by-play/season=20262027/date=2026-10-10/game_2026020001.json"
    ]
    assert scope.shift_paths == [
        "s3a://nhl-bronze/shift-charts/season=20262027/date=2026-10-10/game_2026020001.json"
    ]
