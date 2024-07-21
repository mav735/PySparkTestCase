# Pyspark TestCase

------------------
## Installation:

    pip install pyspark (or requirements)

(Also need JDK)

## Run main.py

Sample data stores in _**data.py**_

## Needed method: _**(analyze_products())**_
    spark = SparkSession.builder \
        .appName("Products and Categories OOP") \
        .getOrCreate()

    manager = ProductCategoryManager(spark)
    #Load samples
    manager.load_data(data.products_data, data.categories_data, data.product_category_data)
    
    product_category_pairs_df, products_without_categories_df = manager.analyze_products()
    print("Product-Category pairs:")
    product_category_pairs_df.show(truncate=False)

    print("Products without categories:")
    products_without_categories_df.show(truncate=False)

    spark.stop()