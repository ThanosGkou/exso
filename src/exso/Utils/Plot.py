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
    def area_plot(df, cols=None, save_path=None, title=None, ylabel=None, xlabel=None, ytick_suffix=None, legend_title=None, show=True, add_total = True, cmap = 'viridis', hovermode = 'x'):

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
        df = df.replace(0, np.NAN)
        if add_total:
            df['TOTAL'] = df.sum(axis= 1)

        cmap_hex = Plot.get_sized_colormap_plotly_compatible(n_colors=df.shape[1], cmap_name=cmap)
        fig = px.area(df, x= df.index, y = df.columns, color_discrete_sequence = cmap_hex)

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
            )
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

###############################################################################################
###############################################################################################
###############################################################################################
