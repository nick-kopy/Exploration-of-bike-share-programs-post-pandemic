### Project overview
An exploratory data analysis of public bikeshare data from major municipal bikeshare programs in a few big cities.

### The story
So I've got a friend in Chicago who is doing work from home. He's thinking about selling his car, but then in the cases where he does go out he would need to rely on other things. Other things like bikes, maybe even rentable bikes.

We got to talking and I told him "You know in LA I don't see the rentals going around anymore, I think people must not be riding them for some reason." To which he replied "Really? The blue rental bikes we got here are still all over the place."

Are the bike share programs in Chicago and LA being hit by the pandemic differently? Is a bigger portion of riders forgoing the rentals in one city vs the other? Let's find out.

### Data description
- LA: 15 columns, by quarter, 800k rows
- CH: 13 columns, by month/quarter, 11m rows

#### TODO
- match up graphs for city comparisons
- when done with notebooks: convert to .py (necessary? but good probably)
  - still use jupyter to move through exploration, but use .py methods
  - make certain processes into functions (when to do class?)
  - include docstrings, make inline comments useful
  - address h testing assumptions
  - end with potential future directions (more cities, correlate w/ other data)
- city data sources, note that I unzipped things
- "representative shard of data" is in repository
- in README include that specific steps can be found in exploration.ipynb
  - maybe end with "we know that they are different but can't say why. Although we couldn't say with this data by itself it sparks an interesting line of investigation" or something
  - include statistically significant results but specifics can stay in ipynb
  - link to student github (interesting)
- presentation
  - practice beforehand, see rubric for specifics
  - identify data sources, show sufficient data for trends to emerge
  - show trends, move to project question (h tests)
  - set  up H tests, give results
  - interpret results, why this matters
  - H tests should be lay, but audience is DS so other stuff can be technical

#### assumptions
- "Programs are consistent over time." These programs are in most cases growing, and in many cities younger than a decade old. LA specifically moved to upgrade a significant portion of their network while ridership was down (https://bikeshare.metro.net/westside-improvement-project/) which in all likelihood exacerbated the pandemic decline.
- "Programs are independent city to city." While LA and Chicago are pretty different, they are not independent. Both are subject to more universal variables like national level bike trends or pressures to reduce carbon emmissions.
- "LA and Chicago's programs are comparable." The truth is as a whole Chicago's system is much bigger. Chicago has about 4 times as many bikes, 6 times as many stations, and 14 times as many bike rides per year. This is in spite of the fact that LA is the more populous city.