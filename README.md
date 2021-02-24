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
- make LA's read in csv's consistent on start time (model after CH), cast to datetype
- hypothesis testing
- match up graphs for city comparisons
- when done with notebooks: convert to .py

#### assumptions
- "Programs are consistent over time." These programs are in most cases growing, and in many cities younger than a decade old. LA specifically moved to upgrade a significant portion of their network while ridership was down (https://bikeshare.metro.net/westside-improvement-project/) which in all likelihood exacerbated the pandemic decline.
- "Programs are independent city to city." While LA and Chicago are pretty different, they are not independent. Both are subject to more universal variables like national level bike trends or pressures to reduce carbon emmissions.