# PySpark Style Guide 🌙
Hey there, fellow PySpark enthusiasts! 👋

Welcome to my humble attempt at a PySpark Style Guide. Before we dive in, I want to be upfront: I'm no coding guru or PySpark wizard. I'm just a regular developer who's spent way too many hours staring at PySpark code, making mistakes, and (occasionally) learning from them.This guide is basically a collection of practices I have developed during my time at [Riaktr](https://riaktr.com/). I've been building data pipelines day in and day out, and these are the patterns that have emerged from my headaches and "aha!" moments.

Now, I'm not claiming these are the "best" practices or the only way to write PySpark code. Far from it! There are probably a million better ways to do things, and I'm sure many of you could code circles around me. This is just my perspective based on my experiences.

So, why bother with a style guide? Well, in my experience, having some common ground on how we write our code has made life a bit easier. It's helped:
- Spend less time scratching my head trying to decipher my own or others' code
- Onboard new team members without them running away in terror
  
If you find anything helpful in here, great! If not, no worries. Feel free to take what resonates, ignore what doesn't, and adapt everything to what works best for you and your team.
Remember, at the end of the day, the best code is the code that gets the job done and remains understandable to you when you revisit it months later.

Happy PySparking! 🫶


## Example
Let's dive into a comprehensive example that demonstrates many of the principles we'll discuss. To make things more fun (and delicious), we'll use a fictional ice cream business scenario:
>**🍦 Our Delicious Data**
>  
> Imagine you're running "Scoops & Smiles," a booming ice cream empire. You're drowning in data: daily sales, flavor popularity, even customer brain freeze incidents. Throughout this guide, we'll use this chilly data to serve up some hot PySpark styling tips.

Here's how we might analyze our ice cream sales data using our PySpark best practices:

```python
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from toolz import curry

PREMIUM_PRICE_THRESHOLD = 4.50
LOW_POPULARITY_THRESHOLD = 50
MEDIUM_POPULARITY_THRESHOLD = 80

scoops_analysis = (
    ice_cream_data
    .transform(remove_incomplete_records)
    .transform(categorize_popularity(LOW_POPULARITY_THRESHOLD, MEDIUM_POPULARITY_THRESHOLD))
    .transform(calculate_daily_revenue)
    .transform(filter_premium_flavors(PREMIUM_PRICE_THRESHOLD))
    .transform(rank_flavors_by_revenue)
)

scoops_analysis = (
    ice_cream_data
    .transform(remove_incomplete_records)
    .transform(categorize_popularity)
    .transform(calculate_daily_revenue)
    .transform(filter_premium_flavors(PREMIUM_PRICE_THRESHOLD))
    .transform(rank_flavors_by_revenue)
)

def remove_incomplete_records(df: DataFrame) -> DataFrame:
    return df.dropna(subset=["flavor", "scoops_sold", "price"])

@curry
def categorize_popularity(low_threshold: int, medium_threshold: int, df: DataFrame) -> DataFrame:
    return df.withColumn("popularity", 
                         F.when(F.col("scoops_sold") < low_threshold, "Give it time to freeze")
                          .when(F.col("scoops_sold") < medium_threshold, "Coolly received")
                          .otherwise("Sizzling hot seller!"))

def calculate_daily_revenue(df: DataFrame) -> DataFrame:
    return df.withColumn("daily_revenue", F.col("scoops_sold") * F.col("price"))

@curry
def filter_premium_flavors(price_threshold: float, df: DataFrame) -> DataFrame:
    return df.filter(F.col("price") >= price_threshold)

def rank_flavors_by_revenue(df: DataFrame) -> DataFrame:
    """
    Rank flavors by their total revenue using a window function.

    We use a window function instead of sorting the entire DataFrame because:
    1. It's more efficient for large datasets, avoiding a full sort.
    2. It allows us to keep all the data while adding the rank, rather than
       potentially losing information with an ORDER BY clause.
    3. It enables easy integration with other analyses that might need the
       unranked data alongside the ranking.
    """
    window_spec = Window.orderBy(F.desc("total_revenue"))
    return (
        df
        .groupBy("flavor")
        .agg(F.sum("daily_revenue").alias("total_revenue"))
        .withColumn("flavor_rank", F.rank().over(window_spec))
    )

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
from pyspark.sql import functions as F

# Good
processed_sales = (
    raw_ice_cream_data
    .transform(clean_sales_data)
    .transform(enrich_with_flavor_info)
    .transform(calculate_daily_scoops)
    .transform(add_temperature_trend(days=7))
    .transform(flag_popular_flavors(scoop_threshold=100))
    .withColumn("processed_date", F.current_date())
)

# Avoid
processed_sales = raw_ice_cream_data.transform(clean_sales_data) \
    .transform(enrich_with_flavor_info) \
    .transform(calculate_daily_scoops) \
    .transform(add_temperature_trend(days=7)) \
    .transform(flag_popular_flavors(scoop_threshold=100)) \
    .withColumn("processed_date", F.current_date())
```

### Minimizing Line Breaks
Aim for concise, single-line transformations whenever possible.  
If a line becomes too long, consider simplifying the transformation instead of breaking it.
```python
# Good
summer_bestsellers = (
    ice_cream_sales
    .filter(F.col("temperature") > 25)
    .filter(F.col("flavor").isin("Vanilla", "Chocolate", "Strawberry"))
    .select("sale_id", "flavor", "scoops_sold", "revenue")
)

# Avoid
summer_bestsellers = (
    ice_cream_sales
    .filter(
        F.col("temperature") > 25
    )
    .filter(
        F.col("flavor").isin(
            "Vanilla",
            "Chocolate",
            "Strawberry"
        )
    )
    .select(
        "sale_id",
        "flavor",
        "scoops_sold",
        "revenue"
    )
)
```

### Put each transformation on its own line
Placing each transformation on a separate line improves readability and makes it easier to modify the pipeline.
```python
# Good
flavor_analysis = (
    ice_cream_sales
    .transform(clean_sales_data)
    .transform(enrich_with_temperature_data)
    .transform(calculate_flavor_metrics)
)

# Avoid
flavor_analysis = (
    ice_cream_sales.transform(clean_sales_data).transform(enrich_with_temperature_data)
    .transform(calculate_flavor_metrics)
)
```

### Embrace functional programming with transform chaining
Pipeline code benefits from functional programming principles, aligning well with the input-process-output nature of data processing. Break down operations into small, focused functions that do only one thing. Use the .transform() method for clear function chaining.
```python
# Good
flavor_analysis = (
    ice_cream_sales
    .transform(clean_sales_data)
    .transform(add_price_category)
    .transform(calculate_flavor_metrics)
)

# Avoid: Procedural approach with intermediate DataFrames
clean_sales = clean_sales_data(ice_cream_sales)
categorized_sales = add_price_category(clean_sales)
flavor_analysis = calculate_flavor_metrics(categorized_sales)
```

### Use currying for multi-argument transformations
When chaining functions that require multiple arguments, use currying with the toolz library. This approach allows you to create transform-compatible functions from multi-argument operations.
```python
from toolz import curry

# Usage in a transformation chain
popular_flavors_analysis = (
    ice_cream_sales
    .transform(clean_sales_data)
    .transform(add_flavor_category("is_classic", ["Vanilla", "Chocolate", "Strawberry"]))
    .transform(add_flavor_category("is_seasonal", ["Pumpkin Spice", "Peppermint"]))
    .transform(filter_by_scoop_count(100))
    .transform(calculate_flavor_metrics)
)

@curry
def add_flavor_category(category_name: str, flavors: list, df: DataFrame) -> DataFrame:
    return df.withColumn(category_name, F.when(F.col("flavor").isin(flavors), "yes").otherwise("no"))

@curry
def filter_by_scoop_count(min_scoops: int, df: DataFrame) -> DataFrame:
    return df.filter(F.col("scoops_sold") >= min_scoops)
```
The @curry decorator from toolz automatically creates a curried version of your function. This allows you to partially apply arguments and still use the function in a .transform() chain. It maintains the clean, functional style of your pipeline while enabling more flexible and reusable transformations.

### Start function names with a verb
Using verbs at the beginning of function names clearly communicates the action being performed.  
Verb-first naming helps create a natural language-like flow when reading code, especially in data pipeline operations. It also aligns with the principle of functions doing one specific thing, as the verb clearly states the primary action of the function.
```python
# Good
def scoop_ice_cream(df: DataFrame) -> DataFrame:
    return df.withColumn("scoops_served", F.col("order_size"))

# Avoid
def ice_cream_scooping(df: DataFrame) -> DataFrame:
    return df.withColumn("scoops_served", F.col("order_size"))

# More examples of good verb-first naming
def melt_inventory(df: DataFrame, temperature_threshold: float) -> DataFrame:
    return df.filter(F.col("storage_temp") > temperature_threshold)

def restock_flavors(df: DataFrame, min_stock: int) -> DataFrame:
    return df.filter(F.col("quantity") < min_stock)

# Examples to avoid
def inventory_melting(df: DataFrame, temperature_threshold: float) -> DataFrame:
    return df.filter(F.col("storage_temp") > temperature_threshold)

def flavors_restocking(df: DataFrame, min_stock: int) -> DataFrame:
    return df.filter(F.col("quantity") < min_stock)

```

### Use type annotation in function
Type annotations improve code clarity, enable better linting, and provide clear hints about function inputs and outputs. They serve as inline documentation and help catch type-related errors early.
```python
from pyspark.sql import DataFrame

# Good
def calculate_daily_scoops(df: DataFrame, flavor_col: str, date_col: str) -> DataFrame:
    return df.groupBy(date_col, flavor_col).agg(F.sum("scoops_sold").alias("total_scoops"))

# Avoid
def calculate_daily_scoops(df, flavor_col, date_col):
    return df.groupBy(date_col, flavor_col).agg(F.sum("scoops_sold").alias("total_scoops"))
```

### Write docstrings to explain the 'why'
Focus on the 'why' rather than the 'how' in docstrings. Only add docstrings to provide context that might not be immediately obvious from the code itself. Do not add docstrings when they're not needed.
```python
# Good - Docstring adds valuable context
def apply_summer_friday_discount(df: DataFrame) -> DataFrame:
    """
    Apply a special discount on Fridays during summer months.

    Our ice cream shop runs a promotion where customers get a 10% discount
    on Fridays in June, July, and August. This is to boost sales on what's
    traditionally been our slowest weekday during peak season.
    """
    return df.withColumn(
        "discounted_price",
        F.when(
            (F.dayofweek("date") == 6) & (F.month("date").isin(6, 7, 8)),
            F.col("price") * 0.9
        ).otherwise(F.col("price"))
    )

# Good - No docstring needed, function is self-explanatory
def calculate_daily_revenue(df: DataFrame) -> DataFrame:
    return df.groupBy("date").agg(F.sum("price" * "scoops_sold").alias("daily_revenue"))

# Avoid - Unnecessary docstring
def get_top_flavors(df: DataFrame, n: int) -> DataFrame:
    """
    This function returns the top N flavors based on the number of scoops sold.
    It groups the data by flavor, sums the scoops sold, and orders the result descendingly.
    """
    return (
        df.groupBy("flavor")
        .agg(F.sum("scoops_sold").alias("total_scoops"))
        .orderBy(F.desc("total_scoops"))
        .limit(n)
    )
```

### Use descriptive name over abbreviation
Opt for clear, descriptive names rather than cryptic abbreviations. Reduce the cognitive load for anyone reading or maintaining the code.  
Descriptive names make your code more intuitive and reduce the need for additional comments. While they may require more typing initially, the clarity they provide pays off in improved maintainability and reduced errors due to misunderstanding. However, be mindful of overly long names that might decrease readability.
```python
# Good
flavor_popularity_ranking = calculate_flavor_popularity(ice_cream_sales_data)

# Avoid
flav_pop_rank = calc_flav_pop(ic_sales)

# More examples

# Good
average_scoops_per_customer = calculate_average_scoops(customer_purchase_history)
best_selling_flavor_by_season = identify_seasonal_favorites(sales_data, weather_data)

# Avoid
avg_scp_cust = calc_avg_scp(cust_purch_hist)
best_flav_season = id_seas_fav(sales, weather)
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

