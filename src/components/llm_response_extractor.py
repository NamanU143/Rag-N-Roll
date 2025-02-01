class FieldExtractor:
    """
    Class to handle the extraction of fields from result objects.
    """

    def __init__(self):
        """
        Initialize the FieldExtractor instance.
        """
        pass  # No logging or additional initialization required

    def extract_fields(self, result):
        """
        Extracts TEXT fields from the result object and concatenates them into a single string.

        Args:
            result (object): Result object containing the query results.

        Returns:
            str: A concatenated string of all TEXT fields, or an empty string if no TEXT fields are found.
        """
        try:
            # Access the results attribute
            records = result.results
            extracted_data = [record.get("TEXT", "") for record in records]

            # Filter out empty strings and concatenate the TEXT fields
            concatenated_string = "".join(filter(None, extracted_data))
            print(concatenated_string, end="")
            
            return concatenated_string
        except Exception as e:
            raise ValueError(f"Error processing result object: {e}")
