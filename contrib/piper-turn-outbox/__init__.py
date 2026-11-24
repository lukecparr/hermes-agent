"""Opt-in Piper outbox settlement extension (in-process only)."""
from gateway.piper_turn_outbox import eligible_turn, insert_completion


def register(ctx):
    configured_route = ctx.get_config('route')
    if not isinstance(configured_route, str) or not configured_route.strip() or configured_route != configured_route.strip():
        raise ValueError('piper-turn-outbox requires a configured route')

    def settle(*, conn, row, route, response, outcome, result):
        agent_result = (result or {}).get('result') or {}
        if agent_result.get('completed') is not True:
            return
        turn_id = eligible_turn(row, route, response, outcome, configured=configured_route)
        if turn_id is not None:
            insert_completion(conn, row['admission_id'], turn_id)

    ctx.register_settlement_extension(settle)
