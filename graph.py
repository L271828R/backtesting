import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Load processed data with merged target_label column.
df = pd.read_csv("data.csv", parse_dates=['datetime'], index_col='datetime')

# Define a color map for label text.
color_map = {'high': 'green', 'low': 'red', 'neutral': 'blue'}

# Create a Plotly figure with a secondary y-axis for volume.
fig = make_subplots(rows=1, cols=1, specs=[[{"secondary_y": True}]],
                    subplot_titles=("30-Min Chart with Session VWAP, VWAP Bands, Volume & 12:30 Labels",))

# 1) Add candlestick chart.
fig.add_trace(go.Candlestick(
    x=df.index,
    open=df['Open'],
    high=df['High'],
    low=df['Low'],
    close=df['Close'],
    name="30-Min Candlesticks"
), row=1, col=1, secondary_y=False)

# 2) Add VWAP line.
fig.add_trace(go.Scatter(
    x=df.index,
    y=df['VWAP'],
    mode='lines',
    name='VWAP (9:30 - next 9:30)',
    line=dict(color='orange', width=2)
), row=1, col=1, secondary_y=False)

# 3) Add VWAP upper band.
fig.add_trace(go.Scatter(
    x=df.index,
    y=df['vwap_upper'],
    mode='lines',
    name='VWAP + 2 STD',
    line=dict(color='orange', dash='dot', width=1)
), row=1, col=1, secondary_y=False)

# 4) Add VWAP lower band.
fig.add_trace(go.Scatter(
    x=df.index,
    y=df['vwap_lower'],
    mode='lines',
    name='VWAP - 2 STD',
    line=dict(color='orange', dash='dot', width=1)
), row=1, col=1, secondary_y=False)

# 5) Add volume bars on secondary y-axis.
fig.add_trace(go.Bar(
    x=df.index,
    y=df['Volume'],
    name='Volume',
    marker_color='lightblue',
    opacity=0.4
), row=1, col=1, secondary_y=True)

# 6) Add target time labels as text.
# Filter the DataFrame to rows where target_label is not null.
target_label_rows = df[df['target_label'].notnull()]
for dt, row in target_label_rows.iterrows():
    label = row['target_label']
    close_price = row['Close']
    fig.add_trace(go.Scatter(
        x=[dt],
        y=[close_price],
        mode="text",
        text=[label],
        textposition="top center",
        textfont=dict(color=color_map.get(label, 'black'), size=12),
        showlegend=False
    ), row=1, col=1, secondary_y=False)

# Update layout.
fig.update_layout(
    title="30-Min Chart with Session VWAP, VWAP Bands, Volume & 12:30 Labels",
    xaxis=dict(type='date', tickformat='%Y-%m-%d %H:%M', showgrid=True),
    yaxis=dict(title='Price', showgrid=True),
    yaxis2=dict(title='Volume', overlaying='y', side='right'),
    hovermode="x unified"
)

fig.show()

