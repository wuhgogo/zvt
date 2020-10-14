# -*- coding: utf-8 -*-

import pandas as pd
from jqdatasdk import auth, logout, get_bars

from zvt import zvt_env
from zvt.api.quote import generate_kdata_id
from zvt.contract import IntervalLevel
from zvt.contract.api import df_to_db
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import Index, Index1dKdata
from zvt.recorders.joinquant.common import to_jq_trading_level, to_jq_entity_id
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, TIME_FORMAT_DAY, TIME_FORMAT_ISO8601


class JqChinaIndexDayKdataRecorder(FixedCycleDataRecorder):
    entity_provider = 'joinquant'
    entity_schema = Index

    # 数据来自jq
    provider = 'joinquant'

    # 只是为了把recorder注册到data_schema
    data_schema = Index1dKdata

    def __init__(self, entity_type='index', exchanges=['sh', 'sz'], entity_ids=None, codes=None, batch_size=10,
                 force_update=False, sleeping_time=10, default_size=2000, real_time=False, fix_duplicate_way='add',
                 start_timestamp=None, end_timestamp=None,
                 level=IntervalLevel.LEVEL_1DAY, kdata_use_begin_time=False, close_hour=0, close_minute=0,
                 one_day_trading_minutes=24 * 60) -> None:
        self.jq_trading_level = to_jq_trading_level(level)
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes)
        auth(zvt_env['jq_username'], zvt_env['jq_password'])

    def get_data_map(self):
        return {}

    def generate_domain_id(self, entity, original_data):
        return generate_kdata_id(entity_id=entity.id, timestamp=original_data['timestamp'], level=self.level)

    def on_finish(self):
        super().on_finish()
        logout()

    def record(self, entity, start, end, size, timestamps):
        if not self.end_timestamp:
            df = get_bars(to_jq_entity_id(entity),
                          count=size,
                          unit=self.jq_trading_level,
                          fields=['date', 'open', 'close', 'low', 'high', 'volume', 'money'],
                          include_now=True)
        else:
            end_timestamp = to_time_str(self.end_timestamp)
            df = get_bars(to_jq_entity_id(entity),
                          count=size,
                          unit=self.jq_trading_level,
                          fields=['date', 'open', 'close', 'low', 'high', 'volume', 'money'],
                          end_dt=end_timestamp,
                          include_now=False)
        if pd_is_not_null(df):
            df['name'] = entity.name
            df.rename(columns={'money': 'turnover', 'date': 'timestamp'}, inplace=True)

            df['entity_id'] = entity.id
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['provider'] = 'joinquant'
            df['level'] = self.level.value
            df['code'] = entity.code

            def generate_kdata_id(se):
                if self.level >= IntervalLevel.LEVEL_1DAY:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_DAY))
                else:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_ISO8601))

            df['id'] = df[['entity_id', 'timestamp']].apply(generate_kdata_id, axis=1)

            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)

        return None


__all__ = ['JqChinaIndexDayKdataRecorder']

if __name__ == '__main__':
    JqChinaIndexDayKdataRecorder().run()
