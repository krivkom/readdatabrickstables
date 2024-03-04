# import time
#
#
# def retry(max_retries, delay=1, backoff=2):
#     def decorator(func):
#         def wrapper(*args, **kwargs):
#             retries = 0
#             while retries < max_retries:
#                 try:
#                     return func(*args, **kwargs)
#                 except Exception as e:
#                     print(f"Error: {e}")
#                     retries += 1
#                     time.sleep(delay * (backoff ** (retries - 1)))
#             raise Exception("Max retries reached. Unable to complete the operation.")
#         return wrapper
#     return decorator
#
#
# # Example usage
# @retry(max_retries=3, delay=2, backoff=2)
# def query_databricks_tables():
#     print(123)
#     raise ConnectionError("Unable to connect to the database")
#
#
# # Call the function with retry
# try:
#     df = query_databricks_tables()
#     print("Success:", df)
# except Exception as e:
#     print("Error:", e)
#

import time
from random import randint


def query_database():
    if randint(0, 10) % 2 == 0:
        return 123
    else:
        # Your database querying logic here
        # Replace the following line with your actual database query logic
        raise Exception("Unable to connect to the database")


def retry_query(max_retries, delay):
    retries = 0
    while retries < max_retries:
        try:
            _df = query_database()
            return _df
        except Exception as e:
            print(f"Error: {e}")
            retries += 1
            time.sleep(delay)

    print("Max retries reached. Unable to complete the operation.")
    return None  # Or raise an exception if needed


# Example usage
df = retry_query(max_retries=3, delay=120)