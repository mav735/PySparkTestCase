from pyspark.sql import SparkSession
from product_category_manager import ProductCategoryManager
import data


def main() -> None:
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Products and Categories OOP") \
        .getOrCreate()

    # Create an instance of the class and load data
    manager = ProductCategoryManager(spark)
    manager.load_data(data.products_data, data.categories_data, data.product_category_data)

    product_category_pairs_df, products_without_categories_df = manager.analyze_products()
    print("Product-Category pairs:")
    product_category_pairs_df.show(truncate=False)

    print("Products without categories:")
    products_without_categories_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
