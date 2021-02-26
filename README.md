### Project overview
An exploratory data analysis of public bikeshare data from two major bike share programs, one in LA and one in Chicago, focusing on the effects of the pandemic.

#### The story
So I've got a friend in Chicago who is doing work from home. He's thinking about selling his car, but then in the cases where he does go out he would need to rely on other things. Other things like bikes, maybe even rentable bikes.

We got to talking and I told him "You know in LA I don't see the rentals going around anymore, I think people must not be riding them for some reason." To which he replied "Really? The blue rental bikes we got here are still all over the place."

Are the bike share programs in Chicago and LA being hit by the pandemic differently? Is a bigger portion of riders forgoing the rentals in one city vs the other? Let's find out.

#### Data description
The data in this analysis comes from public sources:
- LA metro: https://bikeshare.metro.net/about/data/
  - includes 15 columns and 800k rows (across 3 years)
- Chicago divvy: https://www.divvybikes.com/system-data
  - includes 13 columns and 11 million rows (across 3 years)

### Exploratory data analysis
After downloading the data, cleaning it, checking for missing data, and unifying mismatched formats we are ready to dive in and see what the data says.

> For a more complete look at the code and statistics used, please see exploration.ipynb. If you'd like to run the code yourself, check out [Pyspark](https://spark.apache.org/docs/latest/api/python/)!

Let's move on to a visualization so we can get a sense of the shape of the data. First we'll do ride count per hour of day across the year for LA.

Below we can see spikes in ridership at 8am and 5pm, and perhaps a little bit around lunch. This makes sense as many people are probably commuters. But these spikes smooth out in 2020 and become more uniform. If we remove January and February from the data (not shown) the 2020 line looks even smaller and smoother. This is likely in large part to the work-from-home part of the pandemic.

![plot1](graphs/la_hour.png)

Next we'll take a peek at a more specific slicing of the data for Chicago. We'll chart ridership across different weekdays but break riders into pass holding members and anonymous riders.

Here we can also start to see some of the effects of work-from-home. A lot of divvy members are renting much more on the weekdays, again probably commuting. Moving to 2020 however the difference between weekend and weekday starts to smooth out. Interestingly non-member ridership looks like it increased in 2020. This in of itself could be an avenue of future analysis.

![plot2](graphs/chicago_weekday.png)

These graphs are great and some trends are beginning to emerge. But we should compare LA and Chicago on some metric. One graph that's easy to interpret is the number of rides taken each month for different years.

First in LA you can see 2018 and 2019 follow a pretty basic pattern but 2020 really falls flat after COVID starts to take it's hold and societal changes roll in.

![plot3](graphs/la_month.png)

Let's look at Chicago during the same time period.

The features of the data are similar to LA's and there is no missing data to note, so we'll jump straight to the visualization.

While Chicago certainly has a dip a couple months into 2020, they appear to recover and even exceed their 2019 numbers by fall. By looking at the scale of rides taken you can also tell Chicago's program is much bigger. Even in the dead of winter more bikes are being riden than in LA's peak season.

![plot4](graphs/chicago_month.png)

If there's one takeaway from this analysis it's this:
>LA's bike share program took a dive and stayed down, while Chicago's program on the other hand bounced back.

It's not hard to see what's going on visually with these two cities so we could stop just there but we should run some statistical tests for two reasons:
1. We can back up any specific findings with math.
2. If we want to analyze other cities where the impact may not be as clear we have a precedent for analyzing it methodically.

It does get technical from this point forward. Let's decide on some measurement we want to compare and then what specific tests we want to run.

Daily ride counts are a decent measurement, there's 365 observations in a year and it's easy to interpret. We could compare the entire year 2019 to 2020 for each city (not shown: 2020 is statistically different for LA but not for Chicago). But that doesn't tell the whole story, I want to get more granular then that. It looks like both cities felt the impact from COVID but that Chicago recovers, let's test that out by slicing up the year. The pandemic starting taking effect roughly around the border between quarter 1 (January to March) and quarter 2 (April to Jun). So by comparing Q1 and Q2 we can measure the initial impact of COVID. By next comparing Q2 to Q3 we can measure some of the long term effects of COVID.

One important step: daily ride counts already vary a ton (but predictably) between winter and summer so comparing them directly won't mean anything. We can normalize ride counts by taking a day-by-day difference score from 2019 to 2020 (ie 2019-01-01 minus 2020-01-01, one for each day). This is again a normalized spread of how 2020 has grown as compared to 2019. Using these difference scores instead of raw ride counts, we can compare different quarters without the interference of the seasons (or any other regular confounding factor like holidays).

![plot5](graphs/la_diff.png)

That looks good, you can see how changes in 2019 hovered around zero, but the 2020 changes were negative (shrinkage rather than growth).

Pulling up Chicago's 2020 difference scores let's highlight the different quarters.

![plot6](graphs/chicago_diff.png)

Now it's time to lay out all of our hypotheses we will test.

>1. H1: LA ride count difference scores (2020 growth) are statistically significantly different between Q1 and Q2 (initial pandemic impact).
>2. H2: LA ride count difference scores (2020 growth) are statistically significantly different between Q2 and Q3 (long term impact).
>3. H3: Chicago ride count difference scores (2020 growth) are statistically significantly different between Q1 and Q2 (initial pandemic impact).
>4. H4: Chicago ride count difference scores (2020 growth) are statistically significantly different between Q2 and Q3 (long term impact).

Each hypothesis is paired with its own version of the same null hypothesis

>- HN: Any two given quarters are not statistically significantly different.

We'll compare quarter data with two sample t-tests. What should our threshold of statistical significance be? The classic value is alpha = 0.05, but since we are testing 4 hypotheses let's include a Bonferroni correction. Instead we will use 0.05 / 4 or alpha = 0.0125. In short, as long as any p-value from the t-tests is below 0.0125 on any given test, it is statistically significant.

>--LA--
>Q1 mean: 200.56
>Q2 mean: -111.24
>Comparing Q1 and Q2 in LA:
> p = 6.57e-12

>--Chicago--
>Q1 mean: 646.46
>Q2 mean: -5004.43
>Comparing Q1 and Q2 in Chicago:
> p = 1.74e-16

When we compare Q1 (pre-pandemic) to Q2 (post-pandemic) for both LA and Chicago they have p values less than our alpha of 0.0125. We fail to reject the null that quarter ride counts are sampled from the same distribution, and have evidence to support them being different. Because quarter means move from positive to negative, the effect is a negative one.

That is to say ride counts went down from the first to second quarter (backed up by math) and it's pretty safe to say the pandemic was the cause. But this isn't surprising, let's move on to see how each city fared a couple months in.

>--LA--
>Q2 mean: -111.24
>Q3 mean: -550.9
>Comparing Q2 and Q3 in LA:
> p =  1.63e-30

>--Chicago--
>Q2 mean: -5004.43
>Q3 mean: 669.24
>Comparing Q2 and Q3 in Chicago:
> p =  1.25e-12

Once again, our t-test comparisons yield p values lower than our 0.0125 threshold on both accounts. We fail to reject the null that quarter ride counts are sampled from the same distribution, and have evidence to support them being different.

The difference here is that LA moves from bad to worse. Chicago moves from bad to good again. Chicago recovers while LA does not.

### Conclusion

We can say with a pretty good degree of confidence that both cities took a hit to ridership from the pandemic but the longer term impact was different, with Chicago recovering and LA trudging through low numbers. While this is fairly obvious from looking at visualizations, there is a framework here for looking at other big bike share programs such as San Francisco's or New York's.

Also, although our test determined ridership changed it doesn't explain why. The pandemic is the obvious cause but its exact effects require more investigation (recall the members vs non-members above). These results though could be a good catalyst to start an investigation into why Chicago did so well.

#### Assumptions

This analysis operates on top of some assumptions which when it comes down to it are not upheld. So even though the math plays out, when interpreting the results it's important to not overextend.

- "Programs are consistent over time." These programs are in most cases growing, and in many cities younger than a decade old. LA specifically moved to [upgrade](https://bikeshare.metro.net/westside-improvement-project/) a significant portion of their network while ridership was down which in all likelihood exacerbated the pandemic decline.
- "Programs are independent city to city." While LA and Chicago are pretty different, they are not independent. Both are subject to more universal variables like national level bike trends or pressures to reduce carbon emmissions.
- "LA and Chicago's programs are comparable." The truth is as a whole Chicago's system is much bigger. Chicago has about 4 times as many bikes, 6 times as many stations, and 14 times as many bike rides per year. This is in spite of the fact that LA is the more populous city.

#### TODO
- address h testing assumptions
  - end with potential future directions (more cities, correlate w/ other data)
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