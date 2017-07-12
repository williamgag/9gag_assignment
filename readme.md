## Documentation

I use scala and spark to do the assignment, spark is running in local single JVM mode, but can be easily configured to work in distributed/cluster mode with minimal to zero code change on the data transformation code.

You will need `sbt` to compile and build the project, if you don't already have it installed in your system, find the installation guide here: http://www.scala-sbt.org/0.13/docs/Setup.html

### Subreddit Leaderboard

This one is pretty straighforward, load all data and do count grouped by subreddit, author(user), created_date(post_date) and then store the result to a specified file

There tens of thousands subreddit in the data.

To run this, go to the root project and execute the following command:

`$ sbt "run-main com.gag.example.SubredditLeaderboard [path to reddit csv input file] [path to csv output file]"`

### Submission Streak

This requires several steps:
1. Identify new streak by using `lag` window function partitioned on user to compare post date with the previous date, if the previous date is not a day before then it is a new streak
2. Mark every new streak with different id, we use cumulative sum window function to achieve this
3. Now that each streak has unique id, we can simply count the posts on each streak grouped by streak id
4. To find the longest streak, we apply `max` on the count column

To run this, go to the root project and execute following command:

`$ sbt "run-main com.gag.example.SubmissionStreak [path to reddit csv input file] [path to csv output file]"`
