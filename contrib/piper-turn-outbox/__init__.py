"""Opt-in Piper outbox settlement extension (in-process only)."""
from gateway.piper_turn_outbox import eligible_turn, insert_completion
from .preview import sanitize_preview, store_preview


def register(ctx):
    configured_route = ctx.get_config('route')
    include_preview = ctx.get_config('include_preview', False) is True
    if not isinstance(configured_route, str) or not configured_route.strip() or configured_route != configured_route.strip():
        raise ValueError('piper-turn-outbox requires a configured route')

    def settle(*, conn, row, route, response, outcome, result):
        agent_result = (result or {}).get('result') or {}
        if agent_result.get('completed') is not True or agent_result.get('_notification_reply_muted') is True:
            return
        # None is the adapter's "already delivered" return for a streamed reply,
        # not proof that inference produced no answer. Keep delivery suppression
        # unchanged; only eligibility uses the captured, completed answer.
        if response is None and agent_result.get('already_sent') is True:
            response = agent_result.get('final_response')
        turn_id = eligible_turn(row, route, response, outcome, configured=configured_route)
        if turn_id is not None:
            insert_completion(conn, row['admission_id'], turn_id)
            if include_preview:
                store_preview(conn, row['admission_id'], sanitize_preview(agent_result.get('final_response')))

    ctx.register_settlement_extension(settle)
