from lxml import etree
import csv

file_name = "data/discogs_20231001_releases.xml"
# file_name = "data/sample.xml"


def handle_artists(artists):
    ret = []
    for artist in artists:
        artist_dict = {}
        for artist_tag in artist:
            if artist_tag.tag == "id":
                artist_dict["id"] = artist_tag.text
            elif artist_tag.tag == "name":
                artist_dict["name"] = artist_tag.text
        ret.append(artist_dict)
    return ret


events = ("start",)
context = etree.iterparse(file_name, events=events)

with open("data/discogs_20231001_releases.csv", "w") as f:
    w = csv.DictWriter(f=f, fieldnames=["release_id", "release_title"], delimiter="\t")
    w.writeheader()

    for action, elem in context:
        if action == "start" and elem.tag == "release":
            release_id = elem.get("id")
            release = {"release_id": release_id}
            for child in elem:
                tag = child.tag
                if tag == "title":
                    release["release_title"] = child.text
                elif tag == "artists":
                    # release["artists"] = handle_artists(child)
                    pass
            if release.get("release_title") != None:
                w.writerow(release)
