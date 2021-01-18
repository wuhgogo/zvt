# -*- coding: utf-8 -*-

import pandas as pd
from jqdatapy.api import request_jqdata

from zvt import init_log
from zvt.contract.api import df_to_db
from zvt.contract.recorder import Recorder, TimeSeriesDataRecorder
from zvt.domain import BlockStock, Block, Stock
from zvt.recorders.joinquant.common import to_entity_id
from zvt.utils.time_utils import now_pd_timestamp
from zvt.utils.pd_utils import pd_is_not_null


class JqChinaBlockRecorder(Recorder):
    provider = 'joinquant'
    data_schema = Block

    def run(self):
        # get stock blocks from joinquant
        category = 'industry'

        df_block = request_jqdata('get_concepts', parse_dates=['start_date'])
        df_block['entity_id'] = df_block['code'].apply(lambda x: f'block_cn_{x}')
        df_block['id'] = df_block['entity_id']
        df_block.rename(columns={'start_date': 'timestamp'}, inplace=True)
        df_block['timestamp'] = pd.to_datetime(df_block['timestamp'])
        df_block['entity_type'] = 'block'
        df_block['exchange'] = 'cn'
        df_block['list_date'] = df_block['timestamp']
        df_block['category'] = category

        df_to_db(data_schema=self.data_schema, df=df_block, provider=self.provider,
                 force_update=True)

        self.logger.info(f"finish record joinquant blocks:{category}")


class JqChinaBlockStockRecorder(TimeSeriesDataRecorder):
    entity_provider = 'joinquant'
    entity_schema = Block

    provider = 'joinquant'
    data_schema = BlockStock

    def __init__(self, entity_type='block', exchanges=None, entity_ids=None, codes=None, batch_size=10,
                 force_update=True, sleeping_time=5, default_size=2000, real_time=False, fix_duplicate_way='add',
                 start_timestamp=None, end_timestamp=None, close_hour=0, close_minute=0) -> None:
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute)

    def record(self, entity, start, end, size, timestamps):
        df_block_stock = request_jqdata('get_concept_stocks', code=entity.code,
                                        parse_dates=None,
                                        header=None)
        if pd_is_not_null(df_block_stock):
            the_list = []
            for _, stock in df_block_stock.iterrows():
                stock_id = to_entity_id(stock[0], 'stock')
                stock_entity = Stock.query_data(provider=self.provider, entity_id=stock_id).iloc[0]
                stock_code = stock_entity['code']
                block_id = entity.id
                the_list.append({
                    'id': '{}_{}'.format(block_id, stock_id),
                    'entity_id': block_id,
                    'entity_type': 'block',
                    'exchange': entity.exchange,
                    'code': entity.code,
                    'name': entity.name,
                    'timestamp': now_pd_timestamp(),
                    'stock_id': stock_id,
                    'stock_code': stock_code,
                    'stock_name': stock_entity['name']
                })

            if the_list:
                df = pd.DataFrame.from_records(the_list)
                df_to_db(data_schema=self.data_schema, df=df, provider=self.provider,
                         force_update=True)
                self.logger.info('finish recording joinquant BlockStock:{},{}'.format(entity.category, entity.name))


__all__ = ['JqChinaBlockRecorder', 'JqChinaBlockStockRecorder']

if __name__ == '__main__':
    init_log('sina_china_stock_category.log')

    JqChinaBlockStockRecorder(codes=['GN705']).run()