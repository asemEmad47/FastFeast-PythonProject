"""
ReadFromSourceFactory — Factory Pattern.
Inspects file extension → returns ReadFromCSV or ReadFromJSON.
"""
from FastFeastApp.etl.components.read_from_csv  import ReadFromCSV
from FastFeastApp.etl.components.read_from_json import ReadFromJSON


class ReadFromSourceFactory:

    _mapping = {
        "csv": ReadFromCSV,
        "json": ReadFromJSON,
    }

    @staticmethod
    def create_source(file_name: str):
        ext = file_name.rsplit(".", 1)[-1].lower()

        reader_class = ReadFromSourceFactory._mapping.get(ext)

        if not reader_class:
            print("Unsupported file type '.{ext}'")
            return

        return reader_class()
