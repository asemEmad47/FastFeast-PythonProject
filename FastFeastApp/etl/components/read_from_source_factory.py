"""
ReadFromSourceFactory — Factory Pattern.
Inspects file extension → returns ReadFromCSV or ReadFromJSON.
Adding a new format = one new class + one elif. Zero other changes.
"""
from etl.components.read_from_csv import ReadFromCSV
from etl.components.read_from_json import ReadFromJSON


class ReadFromSourceFactory:

    @staticmethod
    def create_source(file_name: str, registry, parser):
        ext = file_name.rsplit(".", 1)[-1].lower()
        audit = getattr(registry, "audit", None)

        if ext == "csv":
            return ReadFromCSV(file_name=file_name, registry=registry, parser=parser, audit=audit)
        elif ext == "json":
            return ReadFromJSON(file_name=file_name, registry=registry, parser=parser, audit=audit)
        else:
            raise ValueError(f"Unsupported file type '.{ext}' for '{file_name}'")
