from lxml import etree

file_name = "data/discogs_20231001_releases.xml"
file_name = "data/sample.xml"

events = ("start", "end")
context = etree.iterparse(file_name, events=events)


for action, elem in context:
    if action == "start" and elem.tag == "release":
        release_id = elem.get("id")
        for child in elem:
            if child.tag in ("title"):
                print(f"BINGO: {release_id} :: {child.text}")
    else:
        print(f".")
