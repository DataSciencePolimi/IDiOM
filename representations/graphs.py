import os
import logging
import datetime
from bokeh.plotting import figure
from datetime import date
from bokeh.embed import components
from bokeh.models import CustomJS, CDSView, IndexFilter, ColumnDataSource
from bokeh.models.widgets import DateRangeSlider, CheckboxGroup, widget
from bokeh.layouts import column, row
from dateutil.relativedelta import relativedelta
from bokeh.models.tools import HoverTool
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator


class Graphs:
    def __init__(self):
        self.LOGGER = self.__get_logger()

    def create_articles_count_per_topic(self, articles_per_month):

        source = ColumnDataSource(articles_per_month)

        self.LOGGER.info(articles_per_month)

        hover = HoverTool(
            tooltips=[
                ("Count", "@top"),
            ],
        )

        plot = figure(
            plot_width=600,
            plot_height=450,
            tools=[hover],
            title="Topic Count per Month",
            x_range=source.data["topic"],
        )

        plot.vbar(
            x=source.data["topic"],
            bottom=0,
            top=source.data["count"],
            width=0.5,
            fill_color=["red", "green", "blue"],
        )

        layout = column(plot)

        # Testing the results
        script, div = components(layout)

        self.LOGGER.info(script)
        self.LOGGER.info(div)

        return layout

    def create_most_frequent_ner(self, mf_ner):

        source = ColumnDataSource(mf_ner)

        self.LOGGER.info(mf_ner)

        hover = HoverTool(
            tooltips=[
                ("Count", "@top"),
            ],
        )

        plot = figure(
            plot_width=600,
            plot_height=450,
            tools=[hover],
            title="Most Frequent Named Entity per Month",
            x_range=source.data["entity_name"],
        )

        plot.vbar(
            x=source.data["entity_name"],
            bottom=0,
            top=source.data["frequency"],
            width=0.5,
            fill_color="red",
            line_color="black",
        )

        layout = column(plot)

        # Testing the results
        script, div = components(layout)

        self.LOGGER.info(script)
        self.LOGGER.info(div)

        return layout

    def create_most_frequent_ner_wordcloud(self, mf_ner):

        source = ColumnDataSource(mf_ner)

        weights = {}
        i = 0

        entity_name = source.data["entity_name"]
        frequency = source.data["frequency"]

        while i < len(entity_name):
            weights[entity_name[i]] = frequency[i]
            i += 1

        print(weights)

        wordcloud = WordCloud().generate_from_frequencies(weights)

        wordcloud.to_file("wordcloud/wordcloud.png")

        return None

    def create_article_ts_mfner(self, articles_with_mfner):

        source = ColumnDataSource(articles_with_mfner)

        hover = HoverTool(
            tooltips=[
                ("Date", "@x{%F}"),
                ("Count", "@top"),
            ],
            formatters={
                "@x": "datetime",
            },
        )

        plot = figure(
            plot_width=600,
            plot_height=450,
            tools=[hover],
            title="Articles including most frequent words per Day",
            x_axis_type="datetime",
        )

        plot.vbar(
            x=source.data["date"],
            bottom=0,
            top=source.data["count"],
            width=60 * 60 * 24 * 1000 * 3 / 4,
            fill_color="red",
            line_color="black",
        )

        layout = column(plot)

        # Testing the results
        script, div = components(layout)

        self.LOGGER.info(script)
        self.LOGGER.info(div)

        return layout

    def create_article_time_series(self, articles_per_day):

        source = ColumnDataSource(articles_per_day)

        hover = HoverTool(
            tooltips=[
                ("Date", "@x{%F}"),
                ("Count", "@top"),
            ],
            formatters={
                "@x": "datetime",
            },
        )

        plot = figure(
            plot_width=600,
            plot_height=450,
            tools=[hover],
            title="Articles per Day",
            x_axis_type="datetime",
        )

        plot.vbar(
            x=source.data["date"],
            bottom=0,
            top=source.data["count"],
            width=60 * 60 * 24 * 1000 * 3 / 4,
            fill_color="red",
            line_color="black",
        )

        layout = column(plot)

        # Testing the results
        script, div = components(layout)

        self.LOGGER.info(script)
        self.LOGGER.info(div)

        return layout

    def fix_date(self, date_array, before_after):

        new_date_array = []

        for el in date_array:
            if before_after == "before":
                new_date_array.append(el - relativedelta(hours=12))
            elif before_after == "after":
                new_date_array.append(el + relativedelta(hours=12))

        return new_date_array

    def create_article_graph_with_sliders_and_filters(self, reduced_articles, start_date):

        # Prepare on-hover tooltip
        hover = HoverTool(
            tooltips=[
                ("Title", "@title"),
                ("Date", "@date{%F}"),
            ],
            formatters={
                "@date": "datetime",
            },
        )

        s_date = datetime.datetime.strptime(start_date, "%Y-%m")
        e_date = s_date + relativedelta(months=1) - datetime.timedelta(days=1)

        # Pick colors depending on the topic
        colors = []
        for topic_array in reduced_articles["topic"]:
            if len(topic_array) == 0:
                continue
            if len(topic_array) == 1:
                colors.append(self.choose_color(topic_array[0]["topic_number"]))
            else:
                max = 0
                max_topic = None
                for topic in topic_array:
                    if topic["topic_prob"] > max:
                        max = topic["topic_prob"]
                        max_topic = topic["topic_number"]

                if max_topic is not None:
                    colors.append(self.choose_color(max_topic))

        reduced_articles["colors"] = colors

        source = ColumnDataSource(reduced_articles)

        # Prepare first index filter based on the date
        index_filter = IndexFilter(
            indices=[x for x in range(0, len(reduced_articles["date"]))]
        )

        callback = CustomJS(
            args=dict(source=source, filter=index_filter),
            code="""
                var date_range_low = new Date(cb_obj.value[0])
                var date_range_high = new Date(cb_obj.value[1])
                date_range_low.setHours(0,0,0,0)
                date_range_high.setHours(0,0,0,0)
                const indices = []
                for (var i=0; i < source.get_length(); i++) {
                    
                    var date_range_data = new Date(source.data.date[i])

                    if (date_range_data >= date_range_low && date_range_data <= date_range_high) {
                        indices.push(i)
                    }
                }

                filter.indices = indices
                source.change.emit()
            """,
        )

        # Prepare slider for date
        date_range_slider = DateRangeSlider(
            value=(
                date(s_date.year, s_date.month, s_date.day),
                date(e_date.year, e_date.month, e_date.day),
            ),
            start=date(s_date.year, s_date.month, s_date.day),
            end=date(e_date.year, e_date.month, e_date.day),
        )
        # Do not set the same event, even on two different filters!
        date_range_slider.js_on_change("value", callback)

        # Prepare second index filter based on the topic
        color_filter = IndexFilter(
            indices=[x for x in range(0, len(reduced_articles["date"]))]
        )

        color_callback = CustomJS(
            args=dict(source=source, filter=color_filter),
            code="""
                var active = this.active
                var labels = this.labels
                var active_labels = []

                for(var j = 0; j < active.length; j++) {
                        active_labels.push(labels[active[j]])
                }

                const indices = []
                for (var i=0; i < source.get_length(); i++) {
                    
                    if (active_labels.includes("Topic 1 - Red") && source.data.colors[i] == 'red' ||
                        active_labels.includes("Topic 2 - Green") && source.data.colors[i] == 'green' ||
                        active_labels.includes("Topic 3 - Blue") && source.data.colors[i] == 'blue') {
                        indices.push(i)
                    }
                }

                filter.indices = indices
                source.change.emit()
            """,
        )

        color_labels = []
        active = []
        active_count = 0

        if "red" in colors:
            color_labels.append("Topic 1 - Red")
            active.append(active_count)
            active_count = active_count + 1
        if "green" in colors:
            color_labels.append("Topic 2 - Green")
            active.append(active_count)
            active_count = active_count + 1
        if "blue" in colors:
            color_labels.append("Topic 3 - Blue")
            active.append(active_count)

        checkbox_group = CheckboxGroup(labels=color_labels, active=active)
        # Do not set the same event, even on two different filters!
        checkbox_group.js_on_click(color_callback)

        # Prepare the final view of the plot
        view = CDSView(source=source, filters=[index_filter, color_filter])

        # Plot
        plot = figure(
            plot_width=600,
            plot_height=600,
            tools=[hover],
            title="Articles",
        )
        plot.scatter(size=8, color="colors", source=source, view=view)

        # Add elements to the final layout to be represented
        layout = column(plot, column(date_range_slider), row(checkbox_group))

        # Testing the results
        script, div = components(layout)

        self.LOGGER.info(script)
        self.LOGGER.info(div)

        return layout

    # Pick the color depending on the topic number that have been assigned
    def choose_color(self, topic_number):
        topic_color = "yellow"
        if int(topic_number) == 0:
            topic_color = "red"
        elif int(topic_number) == 1:
            topic_color = "green"
        elif int(topic_number) == 2:
            topic_color = "blue"

        return topic_color

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("Graphs")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "log/graphs.log"
        if not os.path.isdir("log/"):
            os.mkdir("log/")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger
