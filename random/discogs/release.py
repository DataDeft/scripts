import csv
import pyarrow as pa
import pyarrow.parquet as pq
from xml.sax import parse, ContentHandler


release_base_name = "discogs_20231001_releases"


class DiscogsFileHandler(ContentHandler):
    def __init__(self, *, write_csv_row_release, csv_writer):
        super().__init__()
        self.current_release = None

        self.write_csv_row_release = write_csv_row_release
        self.csv_writer = csv_writer

        self.tag_stack = []

        self.char_buffer = ""

    @property
    def tag_path(self) -> tuple:
        return tuple(self.tag_stack)

    def startElement(self, name: str, attrs):
        self.tag_stack.append(name)
        path = self.tag_path
        if path == ("releases", "release"):
            assert not self.current_release
            release = {
                f"release_{k}": v
                for k, v in {**dict(attrs)}.items()
                if k in ("title", "id")
            }
            self.current_release = release

    def endElement(self, name: str):
        path = self.tag_path

        # Adding title
        if path == ("releases", "release", "title"):
            self.current_release["release_title"] = self.char_buffer.strip()

        # Adding notes
        if path == ("releases", "release", "notes"):
            self.current_release["release_notes"] = self.char_buffer.strip()

        # Writing CSV row when finished constrocting the release object
        if path == ("releases", "release"):
            self.write_csv_row_release(self.csv_writer, self.current_release)
            self.current_release = None

        if path == ("releases",):
            print("AIDDDSSSSSSSSSSSSS")

        assert self.tag_stack.pop() == name
        self.char_buffer = ""

    def characters(self, content: str):
        self.char_buffer += content


def write_csv_row_release(csv_writer, release: dict):
    if release.get("release_title") != None:
        csv_writer.writerow(release)


def write_csv_file():
    f = open(f"data/{release_base_name}.csv", "w")
    csv_writer = csv.DictWriter(
        f=f,
        fieldnames=["release_id", "release_title", "release_notes"],
        delimiter="\t",
    )
    csv_writer.writeheader()

    file_name = f"data/{release_base_name}.xml"
    # file_name = "data/sample.xml"
    parse(
        file_name,
        DiscogsFileHandler(
            write_csv_row_release=write_csv_row_release, csv_writer=csv_writer
        ),
    )

    f.close()


# def write_parq_file():
#     release_schema = pa.schema(
#         [
#             ("release_id", pa.int64()),
#             ("release_title", pa.string()),
#             ("release_notes", pa.string()),
#         ]
#     )
#     parquet_file = pq.ParquetFile(f"data/{release_base_name}.zstd.parq")
#     pq.ParquetWriter.write()


write_csv_file()
