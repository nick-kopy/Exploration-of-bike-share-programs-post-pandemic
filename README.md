### Project overview
An exploratory data analysis of public bikeshare data from major municipal bikeshare programs in a few big cities.

### Data description
- LA: "metro-trips" / "metro-bike-share", 15 columns, by quarter, starts July 2016
- SF: "fordgobike" / "baywheels", 14 columns, by month, starts end 2017
- CH: "divvy-trips", 13 columns, by month/quarter, starts end 2013
- NY: "citibike", 15 columns, by month, starts mid 2013

#### TODO
- get each city into spark DF, then reduce before shipping to CSV/PD for analysis
- find bike_type data (may be newer, not all years)