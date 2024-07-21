from typing import List, Tuple
from pyspark.sql import SparkSession, DataFrame


class ProductCategoryManager:
    """
    Class to manage products and categories in PySpark.

    Attributes:
    ----------
    spark : SparkSession
        Spark session for working with PySpark.
    products_df : DataFrame with product data.
    categories_df : DataFrame with category data.
    product_category_df : DataFrame with product-category relationships.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize the ProductCategoryManager object.

        Parameters:
        ----------
        spark : SparkSession
            Spark session for working with PySpark.
        """
        self.spark = spark
        self.products_df: DataFrame = None
        self.categories_df: DataFrame = None
        self.product_category_df: DataFrame = None

    def load_data(self,
                  products_data: List[Tuple[int, str]],
                  categories_data: List[Tuple[int, str]],
                  product_category_data: List[Tuple[int, int]]) -> None:
        """
        Load data into DataFrames.

        Parameters:
        ----------
        products_data : list of tuples
            List of tuples with product data (product_id, product_name).
        categories_data : list of tuples
            List of tuples with category data (category_id, category_name).
        product_category_data : list of tuples
            List of tuples with product-category relationships (product_id, category_id).
        """
        self.products_df = self.spark.createDataFrame(products_data, ["product_id", "product_name"])
        self.categories_df = self.spark.createDataFrame(categories_data, ["category_id", "category_name"])
        self.product_category_df = self.spark.createDataFrame(product_category_data, ["product_id", "category_id"])

        # Debug output to check data loading
        print("Products DataFrame:")
        self.products_df.show()
        print("Categories DataFrame:")
        self.categories_df.show()

    def get_product_category_pairs(self) -> DataFrame:
        """
        Get pairs of "Product Name - Category Name".

        Returns:
        ----------
        DataFrame with pairs of "Product Name - Category Name".
        """
        product_category_joined_df = self.product_category_df \
            .join(self.products_df, "product_id") \
            .join(self.categories_df, "category_id")
        return product_category_joined_df.select("product_name", "category_name")

    def get_products_without_categories(self) -> DataFrame:
        """
        Get products without categories.

        Returns:
        ----------
        DataFrame with names of products without categories.
        """
        products_with_categories_df = self.product_category_df.select("product_id").distinct()
        products_without_categories_df = self.products_df.join(products_with_categories_df, "product_id", "left_anti")
        return products_without_categories_df.select("product_name")

    def analyze_products(self) -> tuple[DataFrame, DataFrame]:
        return self.get_product_category_pairs(), self.get_products_without_categories()
