"""
ReadFromSourceFactory — Factory Pattern.
Inspects file extension → returns ReadFromCSV or ReadFromJSON.
"""
from etl.components.read_from_csv  import ReadFromCSV
from etl.components.read_from_json import ReadFromJSON
from registry.data_registry        import DataRegistry
from registry.conf_file_parser     import ConfFileParser


class ReadFromSourceFactory:

    @staticmethod
    def create_source(
        file_name: str,
        registry:  DataRegistry,
        parser:    ConfFileParser,
    ):
        ext   = file_name.rsplit(".", 1)[-1].lower()
        audit = getattr(registry, "audit", None)

        if ext == "csv":
            return ReadFromCSV(file_path=file_name, audit=audit, registry=registry)
        elif ext == "json":
            return ReadFromJSON(file_path=file_name, audit=audit, registry=registry)
        else:
            raise ValueError(f"Unsupported file type '.{ext}' for '{file_name}'")
