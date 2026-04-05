"""
ReadFromSourceFactory — Factory Pattern.
Inspects file extension → returns ReadFromCSV or ReadFromJSON.
"""
from etl.components.read_from_csv  import ReadFromCSV
from etl.components.read_from_json import ReadFromJSON
from audit.audit import Audit
from registry.data_registry import DataRegistry

class ReadFromSourceFactory:

    _mapping = {
        "csv": ReadFromCSV,
        "json": ReadFromJSON,
    }

    @staticmethod
    def create_source(file_name: str,audit: Audit, registry: DataRegistry = None):
        ext = file_name.rsplit(".", 1)[-1].lower()

        reader_class = ReadFromSourceFactory._mapping.get(ext)

        if not reader_class:
            print("Unsupported file type '.{ext}'")
            return

        return reader_class(audit, registry)
