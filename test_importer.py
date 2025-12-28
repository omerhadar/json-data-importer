import pytest
import json

from main import JsonImporter

@pytest.fixture
def sample_config(tmp_path):
    """Creates a temporary table_config.json file matching the assignment structure."""
    config = {
        "record_path": "value",
        "table_name": "user_data",
        "columns": {
            "id": "id",
            "mail": "email_address",  # Intentionally mapped to check renaming
            "signInActivity.lastSignInDateTime": "last_signin"
        },
        "nested_tables": []
    }
    config_path = tmp_path / "table_config.json"
    with open(config_path, "w") as f:
        json.dump(config, f)
    return str(config_path)


@pytest.fixture
def sample_data_dir(tmp_path):
    """Creates a temporary directory with two sample JSON files."""
    data_dir = tmp_path / "input_files"
    data_dir.mkdir()

    # File 1
    file1_content = {
        "value": [
            {
                "id": "user1",
                "mail": "user1@test.com",
                "signInActivity": {"lastSignInDateTime": "2023-01-01T10:00:00Z"},
                "extra_field": "should_be_ignored"
            }
        ]
    }

    # File 2 (Two users)
    file2_content = {
        "value": [
            {
                "id": "user2",
                "mail": "user2@test.com",
                "signInActivity": {"lastSignInDateTime": "2023-01-02T10:00:00Z"}
            },
            {
                "id": "user3",
                "mail": "user3@test.com",
                "signInActivity": {"lastSignInDateTime": "2023-01-03T10:00:00Z"}
            }
        ]
    }

    with open(data_dir / "file1.json", "w") as f:
        json.dump(file1_content, f)
    with open(data_dir / "file2.json", "w") as f:
        json.dump(file2_content, f)

    return str(data_dir)


def test_importer_initialization(sample_config, sample_data_dir):
    """
    Test if the class initializes correctly, loads config, and finds files.
    """
    importer = JsonImporter(sample_config, sample_data_dir, num_of_workers=2)

    assert len(importer.files) == 2
    assert importer.table_config['table_name'] == "user_data"


def test_merging_logic(sample_config, sample_data_dir):
    """
    Test if the importer correctly merges data from multiple files.
    We expect 3 total records (1 from file1, 2 from file2).
    """
    importer = JsonImporter(sample_config, sample_data_dir, num_of_workers=2)
    result = importer.parse_files()
    df = result["user_data"]

    # Assert we have 3 total rows
    assert len(df) == 3
    # Assert all IDs are present
    assert set(df['id'].tolist()) == {'user1', 'user2', 'user3'}



def test_flattening_logic(sample_config, sample_data_dir):
    """
    Test the 'signInActivity' transformation.
    Ensures nested JSON paths are correctly flattened into a single column.
    """
    importer = JsonImporter(sample_config, sample_data_dir, num_of_workers=1)
    result = importer.parse_files()
    df = result["user_data"]

    # Check if the nested value was extracted to the top level
    # We look for the user with id 'user1'
    user1_row = df[df['id'] == 'user1'].iloc[0]
    assert user1_row['last_signin'] == "2023-01-01T10:00:00Z"