import uuid
import os
from json_importer import JsonImporter


def main():
    table_config_file_path = os.getenv('TABLE_CONFIG_PATH')
    files_dir = os.getenv('FILES_DIR')

    importer = JsonImporter(
        table_config_file_path=table_config_file_path,
        files_dir=files_dir,
        num_of_workers=4
    )
    tables = importer.parse_files()
    df = tables["user_data"]

    #  Adding an uuid as 'external_id' column for each record
    df['external_id'] = [str(uuid.uuid4()) for _ in range(len(df.index))]
    df.to_json('user_data.json', orient='records')


if __name__ == '__main__':
    main()
