from pyspark import SparkContext
from bs4 import BeautifulSoup
import re
import os
import argparse


def extract_tags_and_key_values(content, tag_name):
    """
    Extracts data between specific HTML tags and key-value pairs from JavaScript.
    """
    soup = BeautifulSoup(content, 'html.parser')
    extracted_data = {}

    # Extract the title tag content (e.g., <title>)
    title_tag = soup.find(tag_name)
    if title_tag:
        extracted_data["title"] = title_tag.text.strip()

    # Extract key-value pairs from JavaScript using a regex pattern
    match = re.search(r"var\s+trackData\s*=\s*\{(.+?)\};", content, re.DOTALL)
    if match:
        js_object = match.group(1).replace("\n", "").strip()
        # Use regex to capture key-value pairs
        pairs = re.findall(r"(\w+)\s*:\s*(['\"].*?['\"]|true|false|null|[0-9]+)", js_object)
        for key, value in pairs:
            cleaned_value = value.strip('"').strip("'").lower() if value in ["true", "false"] else value.strip('"').strip("'")
            extracted_data[key] = cleaned_value

    # Ensure all expected fields are present in the output (add default if missing)
    fields = ["title", "profileId", "recognized", "persistentCart", "guestUser", "stnum", "rzTier", "rzId", "sysDate", "timestamp"]
    for field in fields:
        if field not in extracted_data:
            extracted_data[field] = ""

    return extracted_data


if _name_ == "_main_":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Extract data from HTML files and save as CSV.")
    parser.add_argument("--input_path", required=True, help="Path to the input directory containing HTML files.")
    parser.add_argument("--output_path", required=True, help="Path to save the extracted data in CSV format.")
    parser.add_argument("--tag_name", default="title", help="HTML tag to extract (default: 'title').")
    args = parser.parse_args()

    # Parse the input arguments
    input_path = args.input_path
    output_path = args.output_path
    tag_to_extract = args.tag_name

    # Initialize SparkContext
    sc = SparkContext.getOrCreate()

    # Get all files from the specified directory
    files = [os.path.join(input_path, file) for file in os.listdir(input_path) if os.path.isfile(os.path.join(input_path, file))]

    # Initialize an empty list to store the output for all files
    all_extracted_data = []

    for file_path in files:
        # Read the file content
        rdd = sc.wholeTextFiles(file_path)
        file_content = rdd.map(lambda x: x[1]).collect()[0]  # Collecting the content of the file

        # Extract structured data from the file content
        extracted_data = extract_tags_and_key_values(file_content, tag_to_extract)
        all_extracted_data.append(extracted_data)

    # Define the output headers
    headers = ["title", "profileId", "recognized", "persistentCart", "guestUser", "stnum", "rzTier", "rzId", "sysDate", "timestamp"]

    # Prepare the output string (CSV format)
    csv_rows = [",".join(headers)]  # Add headers as the first row
    for data in all_extracted_data:
        csv_rows.append(",".join([data.get(header, "") for header in headers]))

    # Save the output to the specified file in CSV format
    with open(output_path, "w") as f:
        f.write("\n".join(csv_rows))

    print(f"Output saved to: {output_path}")


