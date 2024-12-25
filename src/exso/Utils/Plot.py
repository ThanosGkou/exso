import re

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.colors
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots
from exso import Files


###############################################################################################
###############################################################################################
###############################################################################################
class Plot:


    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def font_update(generic=None, ax_titles=None, ax_labels=None, xticks=None, yticks=None, legend=None, fig_title=None,
                    text=None):
        if generic:
            plt.rcParams.update({'font.size': 22})
        if text:
            plt.rc('font', size=text)  # controls default text sizes
        if ax_titles:
            plt.rc('axes', titlesize=ax_titles)  # fontsize of the axes title
        if ax_labels:
            plt.rc('axes', labelsize=ax_labels)  # fontsize of the x and y labels
        if xticks:
            plt.rc('xtick', labelsize=xticks)  # fontsize of the tick labels
        if yticks:
            plt.rc('ytick', labelsize=yticks)  # fontsize of the tick labels
        if legend:
            plt.rc('legend', fontsize=legend)  # legend fontsize
        if fig_title:
            plt.rc('figure', titlesize=fig_title)  # fontsize of the figure title

    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def print_plotly_colors():

        named_colorscales = px.colors.named_colorscales()
        for i in named_colorscales:
            print(i)

    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def get_sized_colormap_plotly_compatible(n_colors, cmap_name='viridis'):
        cmap = matplotlib.cm.get_cmap(cmap_name, n_colors)
        cmap_hex = []
        for i in range(cmap.N):
            rgba = cmap(i)
            cmap_hex.append(matplotlib.colors.rgb2hex(rgba))

        return cmap_hex

    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def area_plot(df, cols=None, save_path=None, title=None, ylabel=None, xlabel=None, ytick_suffix=None, legend_title=None, show=True, add_total = False, cmap = 'viridis', hovermode = 'x'):

        if isinstance(df, pd.Series):
            df = df.to_frame(name='Values')
        if not isinstance(cols, type(None)):
            df = df[cols].copy()

        if not title:
            title = ""
        if not save_path:
            save_path = Files._exso_dir / "_plotly_.html"

        if not ytick_suffix:
            ytick_suffix = ""
        if not legend_title:
            legend_title = ""
        if not ylabel:
            ylabel = ""
        if not xlabel:
            xlabel = ""

        df = df.dropna(axis = 'columns', how='all')
        df = df.drop(columns = df.columns[df.sum()==0])
        df = df.replace(0, np.nan)

        remove = list(map(lambda x: re.search('total', x.lower()), df.columns.to_list()))
        remove = [i for i,r in enumerate(remove) if r]
        remove = df.columns[remove]
        df = df.drop(columns = remove, errors='ignore')
        cols = df.columns.to_list()
        # if add_total:
        df['_TOTAL'] = df.sum(axis= 1)

        cmap_hex = Plot.get_sized_colormap_plotly_compatible(n_colors=df.shape[1], cmap_name=cmap)
        fig = px.area(df, x= df.index, y = cols, color_discrete_sequence = cmap_hex)
        fig.add_trace(go.Scatter(x = df.index, y = df['_TOTAL'], name='TOTAL (sum)', mode = 'lines',fillcolor='black', opacity=0.7, line = dict(color = 'black')))


        fig.update_yaxes(ticksuffix=ytick_suffix, showgrid=True, separatethousands=True, tickfont_size=24,
                         linecolor='black', showline=True, gridcolor='darkgray')
        fig.update_xaxes(tickfont_size=24, linecolor='black', showline=True, gridcolor='darkgray')

        fig.update_traces(textposition='top center', hovertemplate=None)
        fig.update_layout(hovermode=hovermode)

        fig.update_layout(
            font_family="Times New Roman",
            font_color="black",
            title_font_family="Times New Roman",
            title_font_color="black",
            legend_title_font_color="black")
            # size = 16,)

        fig.update_layout(
            title={'text': '<b>' + title, 'y': 0.9, 'x': 0.5, 'xanchor': 'center', 'yanchor': 'top'},
            xaxis_title='<b>' + xlabel,
            yaxis_title='<b>' + ylabel,
            legend_title='<b>' + legend_title,
            font=dict(
                family="Times New Roman",
                size=18,
                color="RebeccaPurple"
            ),
            hoverlabel={'font':{'size':12}}
        )

        fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
        fig.update_layout(legend={'traceorder': 'reversed'})
        fig.update_layout({'paper_bgcolor': 'snow',#'rgba(0,0,0,0)',
                           'plot_bgcolor': 'snow', #'rgba(0,0,0,0)'
                           })

        config = {'displayModeBar': True,
                  'scrollZoom': True,
                  'toImageButtonOptions': {'format': 'png',  # one of png, svg, jpeg, webp
                                           'filename': 'custom_image',
                                           'height': 500,  # or None
                                           'width': 700,  # or None
                                           'scale': 1  # Multiply title/legend/axis/canvas sizes by this factor
                                           },
                  # 'modeBarButtonsToRemove': ['zoom', 'pan'],
                  'modeBarButtonsToAdd': ['drawline',
                                          'drawopenpath',
                                          'drawclosedpath',
                                          'drawcircle',
                                          'drawrect',
                                          'eraseshape'
                                          ]
                  }
        if save_path:
            fig.write_html(save_path, config=config)
        if show:
            fig.show(config=config)
        return fig

    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def line_plot(df, cols = None, title = None, xlabel = None, ylabel = None, ytick_suffix = None, show = True, legend_title = None, save_path = None, secondary_y_cols = None, secondary_y_label = None, width = 1800, height = 1000, hovermode = 'x'):

        if isinstance(df,pd.Series):
            df = df.to_frame(name = 'Values')
        if isinstance(cols, type(None)):
            cols = df.columns.to_list()

        if not title:
            title = ""
        if not save_path:
            save_path = Files._exso_dir / "_plotly_.html"
        if not ytick_suffix:
            ytick_suffix = ""
        if not legend_title:
            legend_title = ""
        if not ylabel:
            ylabel = ""
        if not xlabel:
            xlabel = "Date"

        buttons = []
        i = 0

        # fig = px.scatter(df, x='RES', y='MCP')
        # fig.show()
        # sys.exit()
        colors = ['gray', 'purple', 'darkgreen', 'gold', 'cadetblue', 'darkgoldenrod', 'turquoise',
                  'darkkhaki', 'steelblue', 'rebeccapurple', 'rosybrown', 'darkcyan', 'mediumseagreen',
                  'mediumaquamarine', 'darkorange', 'indigo', 'navy', 'royalblue', 'slategray']
        colors += colors + colors + colors + colors + colors

        # fig = go.Figure()

        if secondary_y_cols:
            counter = 0

            fig = plotly.subplots.make_subplots(specs = [[{'secondary_y':True}]])
            df_normal = df.drop(columns=secondary_y_cols)
            if secondary_y_label:
                fig.update_yaxes(title_text=secondary_y_label, secondary_y=True)

            for c in df_normal.columns:
                fig.add_trace(go.Scatter(x = df.index, y=df[c].values, name = c,
                                         line = dict(color = colors[counter]),
                                         mode = 'lines'))
                counter += 1

            # fig = px.line(df_normal, color_discrete_sequence=colors[:df_normal.shape[1]])

            for c in secondary_y_cols:
                fig.add_trace(go.Scatter(x=df.index, y=df[c].values, name=c,
                                         line=dict(color=colors[counter]),
                                         mode='lines'),  secondary_y=True)
                counter += 1

        else:
            fig = px.line(df,color_discrete_sequence=colors[:df.shape[1]])

        fig.update_traces(line = dict(width = 1))
        fig.update_traces(textposition='top center', hovertemplate=None)
        fig.update_layout(hovermode=hovermode)

        fig.update_layout({'paper_bgcolor': 'rgba(0,0,0,0)',
                           'plot_bgcolor': 'snow'})  # rgba(0,0,0,0)'})
        fig.update_layout(title=title,
                          legend_title=legend_title)

        ''' 
        j=0
        for c in cols:
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df[c],
                    name=c,
                    visible=(i == 0),
                    line_color=colors[j]), )
            j+=1

        fig.add_trace(go.Scatter(x = df.index,
                                 y = df,
                                 name = "Show All",
                                 visible=False ))
        

        for c in cols:
            args = [False] * len(cols)
            args[i] = True
            button = dict(label=c,
                          method="update",
                          args=[{"visible": args}])

            # add the button to our list of buttons
            buttons.append(button)
            i += 1

        button = dict(label = "Show All",
                      method = "update",
                      args = [{"visible": [True]*(len(cols)+1)}])
        buttons.append(button)
        '''
        fig.update_layout(updatemenus=[dict(active=0,
                                            type="dropdown",
                                            buttons=buttons,
                                            x=0,
                                            y=1.1,
                                            xanchor='left',
                                            yanchor='bottom'),
                                       ])
        fig.update_layout(
            autosize=False,
            width=width,
            height=height, )

        fig.update_layout(
            font_family="Courier New",
            font_color="black",
            title_font_family="Times New Roman",
            title_font_color="black",
            legend_title_font_color="black", )

        config = {'displayModeBar': True,
                  'scrollZoom': True,
                  # 'dragmode': 'pan',
                  'toImageButtonOptions': {'format': 'png',  # one of png, svg, jpeg, webp
                                           'filename': 'custom_image',
                                           'height': 500,  # or None
                                           'width': 700,  # or None
                                           'scale': 1  # Multiply title/legend/axis/canvas sizes by this factor
                                           },
                  # 'modeBarButtonsToRemove': ['zoom', 'pan'],
                  'modeBarButtonsToAdd': ['drawline',
                                          'drawopenpath',
                                          'drawclosedpath',
                                          'drawcircle',
                                          'drawrect',
                                          'eraseshape'
                                          ]
                  }

        fig.update_yaxes(ticksuffix=ytick_suffix, showgrid=True, separatethousands=True, tickfont_size=24, linecolor = 'black', showline = True, gridcolor = 'darkgray')
        fig.update_xaxes(tickfont_size=24, linecolor='black', showline = True, gridcolor = 'darkgray')
        fig.update_layout(
            title={'text': '<b>' + title, 'y': 0.9, 'x': 0.5, 'xanchor': 'center', 'yanchor': 'top'},
            xaxis_title='<b>' + xlabel,
            yaxis_title='<b>' + ylabel,
            legend_title='<b>' + legend_title,
            font=dict(
                family="Courier New, monospace",
                size=18,
                color="RebeccaPurple"
            )
        )
        fig.write_html(save_path, config = config)


        if show:
            fig.show(config=config)

        return fig

    # ********   *********   *********   *********   *********   *********   *********   *********
    # ********   *********   *********   *********   *********   *********   *********   *********
    @staticmethod
    def multi_chart_type(df, line_cols=None, area_cols = None, initialize_as = 'line',
                   title=None, xlabel=None, ylabel=None, ytick_suffix=None, show=True, legend_title=None,
                   save_path=None, secondary_y_cols=None, secondary_y_label=None, width=1800, height=1000,
                   hovermode='x'):

        if isinstance(df,pd.Series):
            df = df.to_frame(name = 'Values')

        if initialize_as == 'line':
            if not line_cols:
                line_cols = df.columns.to_list()
            else:
                if not area_cols:
                    area_cols = [c for c in df.columns.to_list() if c not in line_cols]
                    # area_cols = []
                else:
                    line_cols = [lc for lc in line_cols if lc not in area_cols]


        elif initialize_as == 'area':
            if not area_cols:
                area_cols = df.columns.to_list()
            else:
                if not line_cols:
                    line_cols = []
                else:
                    area_cols = [ac for ac in area_cols if ac not in line_cols]

        else:
            raise ValueError()

        if not title:
            title = ""
        if not secondary_y_label:
            secondary_y_label = ""
        if not save_path:
            save_path = Files._exso_dir / "_plotly_.html"
        if not ytick_suffix:
            ytick_suffix = ""
        if not legend_title:
            legend_title = ""
        if not ylabel:
            ylabel = ""
        if not xlabel:
            xlabel = "Date"

        fig = plotly.subplots.make_subplots(specs=[[{"secondary_y": True}]])
        fig = plotly.subplots.make_subplots(cols=1, rows=2)


        px.area()
        # fig = go.Figure()


        for col in line_cols:
            fig.add_trace(
                go.Scatter(x = df.index, y = df[col],name = col, yaxis = 'y1')
            )
        i = 0
        for col in area_cols:
            if i == 0:
                fig.add_trace(go.Scatter(x=df.index, y=df[col], name=col, yaxis='y2', fill='tozeroy'))
                i += 1
            fig.add_trace(go.Scatter(x=df.index, y=df[col], name=col, yaxis='y2', fill='tonexty'))


        fig.update_traces(line=dict(width=1),
                          textposition = 'top center',
                          hovertemplate = None)

        fig.update_layout(hovermode=hovermode,
                          paper_bgcolor = 'rgba(0,0,0,0)',
                          plot_bgcolor = 'snow',
                          legend_title = legend_title,
                          autosize = False,
                          width = width,
                          height = height,
                          # font_family="Courier New",
                          # font_color="black",
                          # title_font_family="Times New Roman",
                          title_font_color="black",
                          # legend_title_font_color="black",
                          title={'text': '<b>' + title, 'y': 0.9, 'x': 0.5, 'xanchor': 'center', 'yanchor': 'top'},
                          xaxis_title='<b>' + xlabel,
                          yaxis_title='<b>' + ylabel,
                          # legend_title='<b>' + legend_title,
                          font=dict(
                              family="Courier New",
                              size=18,
                              color="RebeccaPurple"),
                           yaxis2=dict(overlaying='y', side='right', title=secondary_y_label)
                          )

        fig.update_yaxes(ticksuffix=ytick_suffix,
                         showgrid=True,
                         separatethousands=True,
                         tickfont_size=24,
                         linecolor='black',
                         showline=True,
                         gridcolor='darkgray')
        fig.update_xaxes(tickfont_size=24,
                         linecolor='black',
                         showline=True,
                         gridcolor='darkgray')

        config = {'displayModeBar': True,
                  'scrollZoom': True,
                  # 'dragmode': 'pan',
                  'toImageButtonOptions': {'format': 'png',  # one of png, svg, jpeg, webp
                                           'filename': 'custom_image',
                                           'height': 500,  # or None
                                           'width': 700,  # or None
                                           'scale': 1  # Multiply title/legend/axis/canvas sizes by this factor
                                           },
                  # 'modeBarButtonsToRemove': ['zoom', 'pan'],
                  'modeBarButtonsToAdd': ['drawline',
                                          'drawopenpath',
                                          'drawclosedpath',
                                          'drawcircle',
                                          'drawrect',
                                          'eraseshape'
                                          ]
                  }

        fig.write_html(save_path, config = config)

        if show:
            fig.show(config=config)
        return fig

    ###############################################################################################
    ###############################################################################################
    ###############################################################################################
    @staticmethod
    def plot_agg_curves(df, buy_col_name = 'Buy', sell_col_name = 'Sell', quantity_name='QUANTITY', price_name = 'PRICE', xlabel = None, ylabel = None):

        general = pd.DataFrame()

        bn = buy_col_name
        sn = sell_col_name
        qn = quantity_name
        pn = price_name
        # df = df.rename(columns={'Down': 'Buy', 'Up': 'Sell', 'QUANTITY_MW': 'QUANTITY'})

        prev = 0
        q_window = max(df[sn, qn].max(), df[bn, qn].max())
        i = 0

        fig = go.Figure()
        for market_hour in df.index.get_level_values(level=0).unique():
            buys = df[bn].loc[market_hour]
            sells = df[sn].loc[market_hour]

            sells.columns = ['Qsell', 'Psell']
            buys.columns = ['Qbuy', 'Pbuy']
            sells['_hoverSell'] = "Q: " + sells['Qsell'].astype(str) + ", P: " + sells['Psell'].astype(str)
            buys['_hoverBuy'] = "Q: " + buys['Qbuy'].astype(str) + ", P: " + buys['Pbuy'].astype(str)

            sells = sells.sort_values(by='Qsell', ascending=True)
            buys = buys.sort_values(by='Qbuy', ascending=False)
            sells['Qsell'] += q_window * i
            buys['Qbuy'] += q_window * i
            both = pd.concat([buys, sells], axis=1)
            general = pd.concat([general, both], axis=0)
            fig.add_vrect(x0=prev, x1=prev + q_window, name=str(market_hour),
                          # **bids_fig
                          )
            fig.add_trace(go.Scatter(mode='text',
                                     text=f"{str(market_hour)}",
                                     x=[(prev + q_window / 2)], y=[4500],
                                     name=None, showlegend=False))
            prev += q_window
            i += 1
        fig.add_trace(go.Scatter(x=general['Qbuy'], y=general['Pbuy'], hovertext=general['_hoverBuy'],
                                 mode='lines+markers', name=bn, ),
                      )
        fig.add_trace(go.Scatter(x=general['Qsell'], y=general['Psell'],
                                 mode='lines+markers', name=sn,
                                 hovertext=general['_hoverSell'],
                                 ),
                      )
        if not xlabel:
            xlabel = qn
        if not ylabel:
            ylabel = pn
        fig.update_layout({'xaxis_title': xlabel,
                           'yaxis_title': ylabel})
        fig.show()
        return fig


