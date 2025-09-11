import os, sys
import logging
from pathlib import Path
from typing import Any
from datetime import date, time

from streamlit.testing.v1 import AppTest


logger = logging.getLogger(__name__)

repo_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(repo_root))
script = repo_root / "datapipe_ui" / "frontend.py"

os.environ["TEST_MODE"] = "1"

def main():
    # Logging setup
    logging.basicConfig(
        level=logging.INFO,  # or DEBUG
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    def get_command(command_type: str) -> dict[str, Any]:
        if command_type == "stream_trades":
            return {"stream_type": "trades", "tickers": 'SPY', 'exchange': 'US', "duration": 20}
        elif command_type == "stream_quotes":
            return {"stream_type": "quotes", "tickers": 'SPY', 'exchange': 'US', "duration": 20}
        elif command_type == "historical_intraday":
            return {'ticker': 'SPY', 'exchange': 'US', 'interval': '1h', 'start': '2025-07-02 09:30', 'end': '2025-07-03 16:00'}
        elif command_type == "historical_interday":
            return {'ticker': 'VOO', 'exchange': 'US', 'interval': 'd', 'start': '2024-10-25', 'end': '2024-11-04'}
        else:
            raise ValueError(f"Unknown command_type: {command_type}")

    # ---------- Mini-DSL helpers to nuke repetition ----------
    def refresh_exp(at: AppTest, i: int):
        """Re-acquire the expander after state changes."""
        return at.expander[i]

    def next_row_id(state_key: str, used_ids: list[int], at: AppTest) -> int:
        return next(v['row_id'] for v in at.session_state[state_key] if v['row_id'] not in used_ids)

    def row_index(state_key: str, row_id: int, at: AppTest) -> int:
        idxs = [i for i, d in enumerate(at.session_state[state_key]) if d.get('row_id') == row_id]
        assert len(idxs) == 1
        return idxs[0]

    def add_config(exp, add_btn_key: str):
        exp.button(add_btn_key).click().run()
        exp.button(add_btn_key).set_value(False)

    def trigger_and_delete(prefix: str, state_key: str, row_id: int, row_idx: int, at: AppTest):
        trigger_key = f'{prefix}_trigger_{row_id}'
        delete_key  = f'{prefix}_del_{row_id}'
        at.run()
        at.button(trigger_key).click().run()
        assert at.session_state[state_key][row_idx]['flow_run_name'] is not None, 'Flow run failed to create'
        at.run()
        at.button(key=delete_key).click().run()
        assert row_id not in [v['row_id'] for v in at.session_state[state_key]], 'Config not deleted'

    def schedule_and_delete(prefix: str, state_key: str, row_id: int, row_idx: int, at: AppTest):
        schedule_key = f'{prefix}_sched_{row_id}'
        delete_key   = f'{prefix}_del_{row_id}'
        at.run()
        at.button(schedule_key).click().run()
        assert len(at.session_state[state_key][row_idx]['schedules']) != 0, 'Schedules failed to create'
        at.run()
        at.button(key=delete_key).click().run()
        assert row_id not in [v['row_id'] for v in at.session_state[state_key]], 'Config not deleted'

    def add_and_locate(state_key: str, used_ids: list[int], at: AppTest):
        rid = next_row_id(state_key, used_ids, at)
        idx = row_index(state_key, rid, at)
        used_ids.append(rid)
        return rid, idx

    # ---------- Specialized field-fillers kept tiny & isolated ----------
    def set_stream_fields(exp, cmd: dict[str, Any], schedule_checkbox_key: str):
        exp = refresh_exp(at, i)
        exp.checkbox(schedule_checkbox_key).set_value(False).run()
        exp = refresh_exp(at, i)

        exp.text_input("stream_new_ticker").set_value(cmd['tickers'])
        exp.text_input("stream_new_exchange").set_value(cmd['exchange'])
        exp.selectbox("stream_stream_type").set_value(cmd['stream_type'])
        exp.text_input("stream_duration_text").set_value(str(cmd['duration']))
        return exp

    def set_hist_frequency(exp, command_type: str):
        # Two calls required to refresh interval options
        exp.selectbox("hist_new_frequency").set_value('Intraday' if command_type == 'historical_intraday' else 'Interday').run()

    def set_hist_fields(exp, cmd: dict[str, Any], command_type: str, schedule_checkbox_key: str):
        # Frequency toggling to refresh interval choices
        exp = refresh_exp(at, i)
        exp.checkbox(schedule_checkbox_key).set_value(False).run()
        exp = refresh_exp(at, i)

        set_hist_frequency(exp, command_type)
        exp = refresh_exp(at, i)

        set_hist_frequency(exp, command_type)
        # Dates/times
        if command_type == 'historical_intraday':
            s_date, s_time = cmd['start'].split(' ')
            e_date, e_time = cmd['end'].split(' ')
            exp.date_input(key='hist_new_start_date').set_value(date.fromisoformat(s_date))
            exp.date_input(key='hist_new_end_date').set_value(date.fromisoformat(e_date))
            exp.time_input(key='hist_new_start_time').set_value(time.fromisoformat(s_time))
            exp.time_input(key='hist_new_end_time').set_value(time.fromisoformat(e_time))
        else:
            exp.date_input(key='hist_new_start_date').set_value(date.fromisoformat(cmd['start']))
            exp.date_input(key='hist_new_end_date').set_value(date.fromisoformat(cmd['end']))
        exp.text_input("hist_new_ticker").set_value(cmd['ticker'])
        exp.text_input("hist_new_exchange").set_value(cmd['exchange'])
        exp.selectbox("hist_new_interval").set_value(cmd['interval'])
        return exp

    # ---------- Generic drive routines (major redundancy removal) ----------
    def run_add_trigger_delete_cycle(
        *,
        at: AppTest,
        exp,
        state_key: str,
        prefix: str,
        used_ids: list[int],
        schedule_checkbox_key: str,
        add_btn_key: str
    ):
        # No schedule
        if prefix == 'hist':
            exp = set_hist_fields(exp, cmd, command_type, schedule_checkbox_key)
        elif prefix == 'stream':
            exp = set_stream_fields(exp, cmd, schedule_checkbox_key)

        if schedule_checkbox_key:
            exp.checkbox(schedule_checkbox_key).uncheck().run()
        add_config(exp, add_btn_key)
        row_id, row_idx = add_and_locate(state_key, used_ids, at)
        trigger_and_delete(prefix, state_key, row_id, row_idx, at)

        # With default schedule
        at.run()
        if schedule_checkbox_key:
            exp.checkbox(schedule_checkbox_key).check().run()
        add_config(exp, add_btn_key)
        row_id, row_idx = add_and_locate(state_key, used_ids, at)
        schedule_and_delete(prefix, state_key, row_id, row_idx, at)
        exp.checkbox(schedule_checkbox_key).uncheck().run()

    # ---------- Test execution ----------
    at = AppTest.from_file(script)
    at.run()

    for i, exp in enumerate(at.expander):
        logger.info("Opening expander %s:", exp.label)
        try:
            if exp.label == '➕ Add new EODHD stream‐fetch config':
                for command_type in ['stream_trades', 'stream_quotes']:
                    used = []
                    cmd = get_command(command_type)

                    schedule_checkbox_key="stream_use_schedule"
                    run_add_trigger_delete_cycle(
                        at=at,
                        exp=exp,
                        state_key='stream_deploy_configs',
                        prefix='stream',
                        used_ids=used,
                        schedule_checkbox_key=schedule_checkbox_key,
                        add_btn_key="stream_add_cfg_btn",
                    )

            elif exp.label == '➕ Add new EODHD historical‐fetch config':
                for command_type in ['historical_interday', 'historical_intraday']:
                    used = []
                    cmd = get_command(command_type)

                    schedule_checkbox_key="hist_use_schedule"
                    run_add_trigger_delete_cycle(
                        at=at,
                        exp=exp,
                        state_key='hist_deploy_configs',
                        prefix='hist',
                        used_ids=used,
                        schedule_checkbox_key = schedule_checkbox_key,
                        add_btn_key="hist_add_cfg_btn",
                    )

        except Exception as e:
            raise e

if __name__ == "__main__":
    main()
