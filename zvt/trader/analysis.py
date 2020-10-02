# -*- coding: utf-8 -*-

import logging
from typing import List

from sqlalchemy import Column, Float, DateTime, String, Integer
from sqlalchemy.ext.declarative import declarative_base

from zvt.contract.register import register_schema
from zvt.utils.decorator import to_string
from zvt.contract import Mixin
from zvt.trader import TradingSignal, TradingListener
from zvt.api.business_reader import AccountStatsReader

AnalysisBase = declarative_base()


@to_string
class RiskAnalysis(AnalysisBase, Mixin):
    __tablename__ = "risk_analysis"

    # 累计净值
    accumulated_net = Column(Float)

    # 年化收益
    annualized_income_pt = Column(String(length=32))

    # 最大回撤
    maximum_retreat_pt = Column(String(length=32))

    # 最大回撤开始时间
    maximum_retreat_start = Column(DateTime)

    # 最大回撤结束时间
    maximum_retreat_end = Column(DateTime)

    # 年化收益/回撤比
    income_retreat_ratio = Column(Float)

    # 盈利次数
    profit_times = Column(Integer)

    # 亏损次数
    losses_times = Column(Integer)

    # 胜率
    winning_ratio_pt = Column(String(length=32))

    # 每笔交易平均盈亏
    profit_losses_average_pt = Column(String(length=32))

    # 盈亏收益比
    profit_losses_ratio = Column(Float)

    # 单笔最大盈利
    single_largest_profit_pt = Column(String(length=32))

    # 单笔最大跌幅
    single_largest_losses_pt = Column(String(length=32))

    # 最大连续盈利次数
    max_profit_times = Column(Integer)

    # 最大连续亏损次数
    max_losses_times = Column(Integer)


class RiskAnalysisTradingListener(TradingListener):
    logger = logging.getLogger(__name__)

    def __init__(self, trader_name) -> None:
        self.trader_name = trader_name

    def on_trading_finish(self, timestamp):
        reader = AccountStatsReader(trader_names=[self.trader_name])
        df = reader.data_df
        trader_result = df.copy()[['trader_name', 'timestamp', 'all_value']]
        trader_result['all_value_pct'] = trader_result['all_value'].pct_change()
        trader_result['all_value_pct'].fillna(value=0, inplace=True)
        trader_result['all_value_yield'] = (1.0 + trader_result['all_value_pct']).cumprod()

        # 累计净值
        accumulated_net = (trader_result.iloc[-1]['all_value_yield'] / 1) - 1

        # 年化收益
        # pow(x, y),计算x的y次方
        # 年华收益：pow((1 + x), 年数) = 总收益率
        # 日化收益：pow((1 + x), 天数) = 总收益率 => pow(总收益率, 1/天数) = (1+日化收益)
        # pow((1 + 日化收益), 365) = 年华收益
        # 整理得到：年华收益 = pow(总收益率, 365/天数) - 1
        trader_days = (trader_result.iloc[-1]['timestamp'] - trader_result.iloc[0]['timestamp']).days + 1
        annualized_income = pow(trader_result.ix[len(trader_result.index) - 1, 'all_value'] / df.ix[0,
                                                                                                    'all_value'],
                                365.0 /
                                trader_days) - 1
        annualized_income_pt = '{}%'.format(round(annualized_income * 100, 2))

        # 最大回撤 从某一个高点，到之后的某个低点，之间最大的下跌幅度。实际意义：在最最坏的情况下，会亏多少钱。
        # 计算当日之前的资金曲线的最高点
        trader_result['max2here'] = trader_result['all_value_yield'].expanding().max()
        # 计算到历史最高值到当日的跌幅
        trader_result['dd2here'] = trader_result['all_value_yield'] / trader_result['max2here'] - 1
        # 计算最大回撤，以及最大回撤时间
        maximum_retreat_end, max_draw_down = tuple(
            trader_result.sort_values(by=['dd2here']).iloc[0][['timestamp', 'dd2here']])
        maximum_retreat_pt = '{}%'.format(round(max_draw_down * 100, 2))
        maximum_retreat_start = \
            trader_result[trader_result['timestamp'] <= maximum_retreat_end].sort_values(by='all_value_yield',
                                                                                         ascending=False).iloc[0][
                'timestamp']

        # 年化收益/回撤比
        income_retreat_ratio = annualized_income / abs(max_draw_down)

        trader_profit = trader_result[trader_result['all_value_pct'] > 0]
        trader_losses = trader_result[trader_result['all_value_pct'] < 0]

        # 盈利次数
        profit_times = len(trader_profit.index)

        # 亏损次数
        losses_times = len(trader_losses.index)

        # 胜率
        winning_ratio = profit_times / losses_times - 1
        winning_ratio_pt = '{}%'.format(round(winning_ratio * 100, 2))

        # 每笔交易平均盈亏
        profit_losses_average = 1.0
        profit_losses_average_pt = '{}%'.format(round(profit_losses_average * 100, 2))

        # 盈亏收益比
        profit_losses_ratio = ""

        # 单笔最大盈利
        single_largest_profit = trader_profit['all_value_pct'].max()
        single_largest_profit_pt = '{}%'.format(round(single_largest_profit * 100, 2))

        # 单笔最大跌幅
        single_largest_losses = trader_profit['all_value_pct'].min()
        single_largest_losses_pt = '{}%'.format(round(single_largest_losses * 100, 2))

        # 最大连续盈利次数
        trader_result['profit_losses'] = trader_result['all_value_pct'].apply(lambda x: 1 if x > 0 else (-1 if x < 0
                                                                                                         else None))
        trader_result['profit_losses'].fillna(method='ffill', inplace=True)
        trader_profit_losses = list(trader_result['profit_losses'])
        profit_losses_list = []
        profit_losses_num = 1
        for i in range(len(trader_profit_losses)):
            if i == 0:
                profit_losses_list.append(profit_losses_num)
            else:
                if (trader_profit_losses[i] == trader_profit_losses[i - 1] == 1) or (trader_profit_losses[
                                                                                         i] == trader_profit_losses[
                                                                                         i - 1] == -1):
                    profit_losses_num += 1
                else:
                    profit_losses_num = 1
                profit_losses_list.append(profit_losses_num)
        trader_result['profit_losses_list'] = profit_losses_list

        max_profit_times = trader_result[trader_result['profit_losses'] == 1].sort_values(by='profit_losses_list',
                                                                                          ascending=False)[
            'profit_losses_list'].iloc[0]
        # 最大连续亏损次数
        max_losses_times = trader_result[trader_result['profit_losses'] == -1].sort_values(by='profit_losses_list',
                                                                                           ascending=False)[
            'profit_losses_list'].iloc[0]
        analysis = RiskAnalysis(accumulated_net=accumulated_net,
                                annualized_income_pt=annualized_income_pt,
                                maximum_retreat_pt=maximum_retreat_pt,
                                maximum_retreat_start=maximum_retreat_start,
                                maximum_retreat_end=maximum_retreat_end,
                                income_retreat_ratio=income_retreat_ratio,
                                profit_times=profit_times,
                                losses_times=losses_times,
                                winning_ratio_pt=winning_ratio_pt,
                                profit_losses_average_pt=profit_losses_average_pt,
                                profit_losses_ratio=profit_losses_ratio,
                                single_largest_profit_pt=single_largest_profit_pt,
                                single_largest_losses_pt=single_largest_losses_pt,
                                max_profit_times=max_profit_times,
                                max_losses_times=max_losses_times)
        print(analysis)

    def on_trading_signals(self, trading_signals: List[TradingSignal]):
        pass

    def on_trading_signal(self, trading_signal: TradingSignal):
        pass

    def on_trading_open(self, timestamp):
        pass

    def on_trading_close(self, timestamp):
        pass

    def on_trading_error(self, timestamp, error):
        pass


register_schema(providers=['zvt'], db_name='analysis', schema_base=AnalysisBase)

__all__ = ['RiskAnalysis']
