from pyspark import SparkContext
from bs4 import BeautifulSoup
import re


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


if name == "main":
    sc = SparkContext.getOrCreate()
    html_file = "/home/sasi/Datafiles/wizard2.txt"  # Replace with your file path
    tag_to_extract = "title"  # Specify the tag to extract

    # Read the entire file as a single unit
    rdd = sc.wholeTextFiles(html_file)
    file_content = rdd.map(lambda x: x[1]).collect()[0]  # Collecting the content of the file

    # Extract structured data from the entire file content
    extracted_data = extract_tags_and_key_values(file_content, tag_to_extract)

    # Define the output headers
    headers = ["title", "profileId", "recognized", "persistentCart", "guestUser", "stnum", "rzTier", "rzId", "sysDate", "timestamp"]

    # Prepare the output string (CSV format in a text file)
    csv_row = ",".join(headers) + "\n" + ",".join([extracted_data.get(header, "") for header in headers])

    # Save the output to a local file in TXT format
    output_path = "/home/sasi/PycharmProjects/pythonProject" # Replace with your desired output path
    with open(output_path, "w") as f:
        f.write(csv_row)

    print(f"Output saved to: {output_path}")

