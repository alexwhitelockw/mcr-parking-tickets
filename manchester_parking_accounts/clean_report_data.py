import pandas as pd
from pathlib import Path


def extract_unique_column_names(file_paths: str, df: pd.DataFrame) -> list:
    """Extracts a list of unique column names from the original CSV.
    This is used to populate the mapping dictionary."""

    unique_columns = []

    for file_path in file_paths:
        report_data = pd.read_csv(file_path, encoding="unicode_escape", nrows=0)
        unique_columns.append(report_data.columns)

    unique_columns = pd.DataFrame(unique_columns).melt()

    unique_columns = unique_columns.loc[unique_columns["value"].notna(), "value"].drop_duplicates().to_list()

    return unique_columns


def detect_incorrect_column_specification(file_paths: str, mapping: dict) -> list:
    """Check each CSV to ensure columns are correctly specified"""

    incorrect_column_specification = []

    for file_path in file_paths:
        report_data = pd.read_csv(file_path, encoding="unicode_escape", nrows=0)
        for col in report_data.columns:
            column_map = mapping.get(col, None)
            if column_map is None and file_path.name not in incorrect_column_specification:
                incorrect_column_specification.append(file_path.name)

    return incorrect_column_specification


def standardise_column_names(file_path: str, mapping: dict) -> pd.DataFrame:
    df = pd.read_csv(file_path, encoding="unicode_escape")
    df = df.rename(columns=mapping)
    return df


if __name__ == "__main__":

    report_data_filepaths = list(Path("data/source").glob("*.csv"))

    mapping = {
        "Issued": "issued_at",
        "Issued At": "issued_at",
        "ï»¿Issued At": "issued_at",
        "Issued Time/Date": "issued_at",
        "Organisation name": "organisation_name",
        "ï»¿Issued Time/Date": "issued_at",
        "Paid": "paid",
        "Organisation Code": "organisation_code",
        "Status": "status",
        "Location": "location",
        "Location ": "location",
        "Ward": "ward",
        "Ticket Destination": "ticket_destination",
        "Ticket Destinatio": "ticket_destination",
        "Ticket destination": "ticket_destination",
        "Zone Name": "zone_name",
        "CEO": "zone_name_two",
        "Zone name": "zone_name",
        "OS Bal": "outstanding_balance",
        "O/S Balance": "outstanding_balance",
        "Restriction": "zone_name",
        "Zone desc": "zone_description",
        "Outstanding": "outstanding_balance",
        "OS Balance": "outstanding_balance",
        "Outstanding Balance": "outstanding_balance",
        "Cont.": "contravention_code",
        "Contravention": "contravention_code",
        "Cont": "contravention_code",
        "Offence": "contravention_code",
        "Contravention Code": "contravention_code",
        "Contravention Description": "contravention_description"
        }

    misspecified_files = detect_incorrect_column_specification(report_data_filepaths, mapping)

    if misspecified_files:
        raise ValueError(f"Column names misspecified for: {misspecified_files}")

    standardised_data = pd.concat([
        standardise_column_names(file_path, mapping)
        for file_path
        in report_data_filepaths])
    
    