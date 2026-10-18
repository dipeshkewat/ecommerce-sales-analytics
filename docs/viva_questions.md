# Viva Questions and Answers

1. **Why use Hadoop MapReduce?** HDFS demonstrates distributed file storage, and Java mapper/reducer tasks demonstrate batch aggregation, sorting, and a keyed lookup join.
2. **Why is Python not used to calculate revenue totals?** Python validates and orchestrates only. Revenue, counts, ranking, and profit are computed by the Java MapReduce jobs and read from their HDFS outputs.
3. **How are distinct orders counted?** Each reducer keeps the exact set of encoded order IDs for the key and reports its cardinality. It does not equate line count with order count.
4. **Why use integer cents?** Integer minor units avoid binary floating-point rounding in financial sums.
5. **How is a global ranking guaranteed?** ProductRanking uses one reducer and a composite sort key with inverted revenue/quantity followed by product ID for deterministic ties.
6. **What happens when a product lookup is missing?** The join emits a `MISSING` record and the dashboard reports its count; uploaded lookup misses are not hidden.
7. **When are profit metrics displayed?** Only when the product lookup contains a complete valid `unit_cost` column. The Java join computes line profit and reducer summaries.
8. **How does the application avoid stale metrics?** It clears the current result when source data or filters change, and publishes metrics only when all three Hadoop jobs and output parsing succeed.
9. **What are the scalability limitations?** Global ranking and profit totals use a single reducer; exact distinct sets consume reducer memory for high-cardinality groups.
10. **Is the sample real retailer data?** No. It is deterministic synthetic data generated for functional demonstration and testing.