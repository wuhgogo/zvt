import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from zvt import init_plugins
from zvt.ui import zvt_app
from zvt.ui.apps import factor_app, trader_app


def serve_layout():
    layout = html.Div(
        children=[
            dcc.Interval(
                id='interval-component',
                interval=60 * 60 * 1000,  # in milliseconds
                n_intervals=0
            ),
            # banner
            html.Div(
                className="zvt-banner",
                children=html.H2(className="h2-title", children="ZVT")
            ),
            # nav
            html.Div(
                className="zvt-nav",
                children=[html.Button('factor', id='btn-factor', n_clicks=0),
                          html.Button('trader', id='btn-trader', n_clicks=0)]
            ),
            html.Div(id='app-content', className="row app-body")
        ]
    )

    return layout


@zvt_app.callback(Output('app-content', 'children'),
                  [Input('btn-factor', 'n_clicks'),
                   Input('btn-trader', 'n_clicks')])
def displayClick(btn1, btn2):
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    if 'btn-factor' in changed_id:
        return factor_app.factor_layout()
    elif 'btn-trader' in changed_id:
        return trader_app.trader_layout()


zvt_app.layout = serve_layout


def main():
    init_plugins()
    zvt_app.run_server(debug=True)


if __name__ == '__main__':
    main()
