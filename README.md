# PySpark Style Guide

This guide outlines my writing style for pipeline code using PySpark. My code formatting comes from years of building data pipelines every day at Riaktr. Feel free to send me a message if you disagree on ideas. I'm always open to learning more.


### Imports

I use aliases for common PySpark modules. This makes it easy to differentiate between Python built-in functions and PySpark functions.

```python
# Good
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# Bad
from pyspark.sql.functions import col, when, lit, sum, avg

```

### DataFrame Operations

I prefer using PySpark's method chaining capabilities and the `transform` method for complex operations.

```python
# Good
result_df = (
    df
    .filter(F.col("age") >= 18)
    .group_by("city")
    .agg(F.avg("salary").alias("avg_salary"))
    .transform(add_population_density)
)

# Bad
temp_df = df.filter(F.col("age") >= 18)
grouped_df = temp_df.groupBy("city")
result_df = grouped_df.agg(F.avg("salary").alias("avg_salary"))
result_df = add_population_density(result_df)

```

## Code Structure and Formatting

### Function Chaining

I use the `.transform()` method for chaining functions. Combined with good function naming, this makes my abstractions easy to understand. I always keep one method per line for better readability.

```python
# Good
result_df = (
    customer_data
    .transform(clean_customer_data)
    .transform(enrich_with_location_data)
    .transform(calculate_customer_metrics)
)

# Bad
result_df = customer_data.transform(clean_customer_data).transform(enrich_with_location_data).transform(calculate_customer_metrics)

```

### Line Breaks and Parentheses

I use parentheses to chain multiple functions and limit line breaks as much as possible. This maintains the flow for readers trying to understand the abstraction.

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

# Bad
processed_sales = raw_sales_data.transform(clean_data) \\
    .transform(enrich_with_customer_info) \\
    .transform(calculate_daily_totals) \\
    .transform(add_moving_average(window_size=7)) \\
    .transform(flag_anomalies(threshold=2.0)) \\
    .withColumn("processed_date", F.current_date())

```

### Minimizing Line Breaks

I limit line breaks as much as possible. Excessive line breaks can disrupt the flow for users trying to read and understand the abstraction. This principle applies not only to function chaining but to all aspects of code structure.

```python
# Good
filtered_data = (
    raw_data
    .filter(F.col("age") > 18)
    .filter(F.col("country").isin("USA", "Canada", "Mexico"))
    .select("user_id", "name", "age", "country")
)

# Bad
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

By minimizing unnecessary line breaks, the code becomes more compact and easier to comprehend at a glance. This approach helps maintain the logical flow of operations and makes it easier to understand the overall structure of the data transformation.

## Functional Programming Principles

I'm a big believer that pipeline code should be written using functional programming principles. This suits the input-process-output nature of data pipelines perfectly.

### Function Design

1. I split my code into small, meaningful functions that do one thing and do it well.
2. My functions always start with a verb.
3. I use `snake_case` for function names.

```python
# Good
def clean_customer_data(df: DataFrame) -> DataFrame:
    return df.dropna(subset=["customer_id", "email"])

# Bad
def customer_data(df: DataFrame) -> DataFrame:
    return df.dropna(subset=["customer_id", "email"])

# Bad
def cleanCustomerData(df: DataFrame) -> DataFrame:
    return df.dropna(subset=["customer_id", "email"])

```

### Type Annotations

I always use type annotations. They improve linting and provide users with clues about the objects I'm working with.

```python
# Good
def calculate_total_sales(df: DataFrame, date_col: str) -> DataFrame:
    return df.groupBy(date_col).agg(F.sum("sales").alias("total_sales"))

# Bad
def calculate_total_sales(df, date_col):
    return df.groupBy(date_col).agg(F.sum("sales").alias("total_sales"))

```

### Docstrings

I add concise docstrings to explain the purpose of my functions. I focus on the 'why' rather than the 'how'.

```python
# Good
def identify_high_value_customers(df: DataFrame, threshold: float) -> DataFrame:
    """
    Identifies high-value customers based on their total purchase amount.

    High-value customers are those whose total purchases exceed the specified threshold.
    This segmentation helps in targeting marketing efforts and personalized services.
    """
    pass

# Bad
def identify_high_value_customers(df: DataFrame, threshold: float) -> DataFrame:
    """
    Identifies high-value customers.

    Args:
    df (DataFrame): Input DataFrame with customer purchase data.
    threshold (float): The purchase amount threshold.

    Returns:
    DataFrame: DataFrame with high-value customers.
    """
    pass
```

## Naming Conventions

I use consistent naming conventions to improve code readability:

1. Functions: `snake_case`, starting with a verb (e.g., `calculate_total_sales`)
2. Variables: `snake_case` (e.g., `customer_data`, `sales_report`)
3. Constants: `UPPER_CASE_WITH_UNDERSCORES` (e.g., `MAX_RETRY_ATTEMPTS`)
4. Classes: `PascalCase` (e.g., `CustomerSegmentation`)

I also prefer using full, descriptive names over abbreviations:

```python
# Good
customer_lifetime_value = calculate_lifetime_value(customer_data)

# Bad
clv = calc_ltv(cust_data)
```

Remember, this style guide is a living document. As I encounter new patterns or best practices, I don't hesitate to update and expand it. Happy coding!
