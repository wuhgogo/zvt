# -*- coding: utf-8 -*-

import dash_core_components as dcc
import dash_html_components as html
from dash import dash
from dash.dependencies import Input, Output

from zvt.factors.factor import factor_cls_registry
from zvt.ui import zvt_app
from zvt.utils.time_utils import TIME_FORMAT_DAY, now_pd_timestamp


def factor_layout():
    layout = html.Div(
        [
            # controls
            html.Div(
                className="three columns card",
                children=[
                    html.Div(
                        className="bg-white user-control",
                        children=[
                            html.Div(
                                className="padding-top-bot",
                                children=[
                                    html.H6("select factor:"),
                                    dcc.Dropdown(id='factor-selector',
                                                 placeholder='select the factor',
                                                 options=[{'label': name, 'value': name} for name in
                                                          factor_cls_registry.keys()]
                                                 ),
                                ],
                            ),
                            html.Div(
                                className="padding-top-bot",
                                children=[
                                    html.H6("select factor target:"),
                                    dcc.Dropdown(id='code-selector',
                                                 placeholder='select the target'
                                                 ),
                                ],
                            ),

                            html.Div(
                                className="padding-top-bot",
                                children=[
                                    # time range filter
                                    dcc.DatePickerRange(
                                        id='date-picker-range',
                                        start_date='2009-01-01',
                                        end_date=now_pd_timestamp(),
                                        display_format=TIME_FORMAT_DAY
                                    )
                                ],
                            ),
                        ])
                ]),
            # Graph
            html.Div(
                className="nine columns card-left",
                children=[
                    html.Div(
                        id='factor-details'
                    )
                ])
        ]
    )

    return layout


@zvt_app.callback(
    Output('factor-details', 'children'),
    [Input('factor-selector', 'value'),
     Input('code-selector', 'value')])
def update_factor_details(factor, code):
    if factor is not None:
        return dcc.Graph(
            id=f'000338-factor',
            figure=factor_cls_registry[factor](codes=['000338']).draw(show=False, height=800))
    raise dash.PreventUpdate()
