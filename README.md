# PySpark Style Guide 🌙
Hey there, fellow PySpark enthusiasts! 👋

Welcome to my humble attempt at a PySpark Style Guide. Before we dive in, I want to be upfront: I'm no coding guru or PySpark wizard. I'm just a regular developer who's spent way too many hours staring at PySpark code, making mistakes, and (occasionally) learning from them.

This guide is basically a collection of practices I have developed during my time at [Riaktr](https://riaktr.com/). I've been building data pipelines day in and day out, and these are the patterns that have emerged from my headaches and "aha!" moments.

Now, I'm not claiming these are the "best" practices or the only way to write PySpark code. Far from it! There are probably a million better ways to do things, and I'm sure many of you could code circles around me. This is just my perspective based on my experiences.

So, why bother with a style guide? Well, in my experience, having some common ground on how we write our code has made life a bit easier. It's helped:
- Spend less time scratching my head trying to decipher my own or others' code
- Onboard new team members without them running away in terror
  
If you find anything helpful in here, great! If not, no worries. Feel free to take what resonates, ignore what doesn't, and adapt everything to what works best for you and your team.
Remember, at the end of the day, the best code is the code that gets the job done and remains understandable to you when you revisit it months later.

Happy PySparking! 🫶

## Example
Before diving into specific guidelines, let's look at a comprehensive example that demonstrates many of the principles we'll discuss:
```python
provide an example here ...
```

## Guidelines

### Import Pyspark modules as aliases
This practice helps distinguish PySpark functions from Python built-ins, making code easier to read and understand.  
Using 'F.' as a prefix clearly indicates PySpark-specific operations, which is especially useful in complex transformations.
```python
# Good
from pyspark.sql import functions as F

# Avoid
from pyspark.sql.functions import col, when, lit, sum, avg
```

### Use parentheses for line breaks
This approach makes it easier to add, remove, or reorder transformations without worrying about line continuation characters.
```python
# Good
processed_sales = (
    raw_sales_data
    .transform(clean_data)
    .transform(enrich_with_customer_info)
    .transform(calculate_daily_totals)
    .transform(add_moving_average(window_size=7))
    .transform(flag_anomalies(threshold=2.0))
    .withColumn("processed_date", F.current_date())
)

# Avoid
processed_sales = raw_sales_data.transform(clean_data) \
    .transform(enrich_with_customer_info) \
    .transform(calculate_daily_totals) \
    .transform(add_moving_average(window_size=7)) \
    .transform(flag_anomalies(threshold=2.0)) \
    .withColumn("processed_date", F.current_date())
```

### Minimizing Line Breaks
Aim for concise, single-line transformations whenever possible.  
If a line becomes too long, consider simplifying the transformation instead of breaking it.
```python
# Good
filtered_data = (
    raw_data
    .filter(F.col("age") > 18)
    .filter(F.col("country").isin("USA", "Canada", "Mexico"))
    .select("user_id", "name", "age", "country")
)

# Avoid
filtered_data = (
    raw_data
    .filter(
        F.col("age") > 18
    )
    .filter(
        F.col("country").isin(
            "USA",
            "Canada",
            "Mexico"
        )
    )
    .select(
        "user_id",
        "name",
        "age",
        "country"
    )
)
```

### Put each transformation on its own line
Placing each transformation on a separate line improves readability and makes it easier to modify the pipeline.
```python
# Good
result_df = (
    customer_data
    .transform(clean_customer_data)
    .transform(enrich_with_location_data)
    .transform(calculate_customer_metrics)
)

# Avoid
result_df = (
    customer_data.transform(clean_customer_data).transform(enrich_with_location_data)
    .transform(calculate_customer_metrics)
)
```

### Embrace functional programming with transform chaining
Pipeline code benefits from functional programming principles, aligning well with the input-process-output nature of data processing. Break down operations into small, focused functions that do only one thing. Use the .transform() method for clear function chaining.
```python
# Good
result_df = (
    customer_data
    .transform(clean_customer_data)
    .transform(enrich_with_location_data)
    .transform(calculate_customer_metrics)
)

# Avoid
result_df = customer_data.transform(clean_customer_data).transform(enrich_with_location_data).transform(calculate_customer_metrics)
```

### Use currying for multi-argument transformations
When chaining functions that require multiple arguments, use currying with the toolz library. This approach allows you to create transform-compatible functions from multi-argument operations.
```python
from toolz import curry

@curry
def add_column_with_default(column_name, default_value, df):
    return df.withColumn(column_name, F.lit(default_value))

@curry
def filter_by_value(column_name, value, df):
    return df.filter(F.col(column_name) == value)

# Usage in a transformation chain
result_df = (
    customer_data
    .transform(clean_customer_data)
    .transform(add_column_with_default("status", "active"))
    .transform(add_column_with_default("registration_date", F.current_date()))
    .transform(filter_by_value("country", "USA"))
    .transform(calculate_customer_metrics)
)
```
The @curry decorator from toolz automatically creates a curried version of your function. This allows you to partially apply arguments and still use the function in a .transform() chain. It maintains the clean, functional style of your pipeline while enabling more flexible and reusable transformations.

### Start function names with a verb
Using verbs at the beginning of function names clearly communicates the action being performed.  
Verb-first naming helps create a natural language-like flow when reading code, especially in data pipeline operations. It also aligns with the principle of functions doing one specific thing, as the verb clearly states the primary action of the function.
```python
# Good
def clean_customer_data(df: DataFrame) -> DataFrame:
    return df.dropna(subset=["customer_id", "email"])

# Avoid
def customer_data_cleaning(df: DataFrame) -> DataFrame:
    return df.dropna(subset=["customer_id", "email"])
```

### Use type annotation in function
Type annotations improve code clarity, enable better linting, and provide clear hints about function inputs and outputs. They serve as inline documentation and help catch type-related errors early.
```python
# Good
def calculate_total_sales(df: DataFrame, date_col: str) -> DataFrame:
    return df.groupBy(date_col).agg(F.sum("sales").alias("total_sales"))

# Avoid
def calculate_total_sales(df, date_col):
    return df.groupBy(date_col).agg(F.sum("sales").alias("total_sales"))
```

### Write docstrings to explain the 'why'
Focus on the 'why' rather than the 'how' in docstrings. Only add docstrings to provide context that might not be immediately obvious from the code itself. Do not add docstrings when they're not needed.
```python
# Good - Docstring adds valuable context
def remove_incoming_onnet_duplicates(df: DataFrame) -> DataFrame:
    """
    Remove duplicate records for incoming, on-network calls.

    In some telecom datasets, incoming on-network calls are recorded twice:
    once for the sender and once for the receiver. This function keeps only
    one record for such calls, the outgoing record.
    """
    pass

# Good - No docstring needed, function is self-explanatory
def add_timestamp_column(df: DataFrame) -> DataFrame:
    return df.withColumn("timestamp", F.current_timestamp())

# Avoid - Unnecessary docstring
def calculate_total_sales(df: DataFrame) -> DataFrame:
    """
    This function calculates the total sales by summing the 'sales' column.
    """
    return df.agg(F.sum("sales").alias("total_sales"))
```

### Use descriptive name over abbreviation
Opt for clear, descriptive names rather than cryptic abbreviations. Reduce the cognitive load for anyone reading or maintaining the code.  
Descriptive names make your code more intuitive and reduce the need for additional comments. While they may require more typing initially, the clarity they provide pays off in improved maintainability and reduced errors due to misunderstanding. However, be mindful of overly long names that might decrease readability.
```python
# Good
customer_lifetime_value = calculate_lifetime_value(customer_data)

# Avoid
clv = calc_ltv(cust_data)
```

## Adios Amigos 🫡
Well, folks, we've reached the end of this PySpark style adventure! 🎢

I hope you've found at least a nugget or two of useful info in this guide. Remember, these are just the practices that have kept me (mostly) sane while wrangling data pipelines. They're not set in stone, and they're definitely not the only way to write PySpark code.

As I continue my journey through the world of big data, I'm sure I'll stumble upon new insights, face-palm at my past mistakes, and maybe even have a few more "eureka!" moments. When that happens, you can bet I'll be updating this guide.

So, consider this a living document – it's growing, changing... Feel free to check back now and then to see what new wisdom (or blunders) I've added.

If you've made it this far, congratulations! You now know way too much about how I like to format my PySpark code. Use this knowledge wisely, or don't use it at all.

Remember, the best code is the one that works, doesn't make your future self want to time-travel just to slap your past self, and hopefully makes your data pipeline purr like a well-oiled machine.

Keep sparking, keep learning, and may your clusters always be distributed and your data never corrupted!

Happy PySparking, everyone! 🐍

