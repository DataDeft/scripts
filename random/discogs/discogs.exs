Mix.install([
  {:stream_gzip, "~> 0.4"},
  {:sweet_xml, "~> 0.7"},
  {:arrow, git: "https://github.com/treebee/elixir-arrow.git"}
])

defmodule Http do
  def save_to_file(url, file_path) do
    IO.inspect("Downloading #{url} to #{file_path}")
    :httpc.request(:get, {String.to_charlist(url), []}, [], stream: String.to_charlist(file_path))
  end
end

defmodule Helpers do
  def log(xs, message) do
    IO.inspect("#{message} : #{xs}")
    xs
  end

  def stream_hash_file(file_path) do
    File.stream!(file_path)
    |> Enum.reduce(:crypto.hash_init(:sha256), &:crypto.hash_update(&2, &1))
    |> :crypto.hash_final()
    |> Base.encode16()
    |> String.downcase()
  end
end

:inets.start()
:ssl.start()

base_url = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2023"

checksum = "discogs_20231001_CHECKSUM.txt"
artists = "discogs_20231001_artists.xml.gz"
labels = "discogs_20231001_labels.xml.gz"
masters = "discogs_20231001_masters.xml.gz"
releases = "discogs_20231001_releases.xml.gz"

IO.inspect([checksum, artists, labels, masters, releases])

data_folder = File.mkdir("data")

case data_folder do
  :ok -> IO.inspect("data/ folder has been created")
  {:error, :eexist} -> IO.inspect("data/ folder has been previously created")
  _ -> IO.inspect("data folder creation has failed: #{inspect(data_folder)}")
end

files = [
  checksum,
  artists,
  labels,
  masters,
  releases
]

# Downloading

files_to_download =
  files
  |> Enum.filter(fn file ->
    Path.join("data", file)
    |> File.exists?()
    |> Kernel.not()
  end)

case files_to_download do
  [] ->
    IO.inspect("Nothing to download")

  xs ->
    xs
    |> Helpers.log("Downloading the following: ")
    |> Enum.each(fn file -> Http.save_to_file("#{base_url}/#{file}", Path.join("data", file)) end)
end

# Verifying

checksum_content =
  Path.join("data", checksum)
  |> File.read!()
  |> String.split("\n", trim: true)
  |> Enum.map(&String.split(&1, "\ "))

IO.inspect(checksum_content)

# [
#   [
#     "583596bf5f05c23ec53...",
#     "discogs_20231001_releases.xml.gz"
#   ],
#   [
#     "b8b924434edcf83e0c5...",
#     "discogs_20231001_masters.xml.gz"
#   ]
# ]

# Broken

# checksum_content
# |> Enum.each(fn [h, f] ->
#   hash_calculated = Helpers.stream_hash_file("data/#{f}")
#   IO.inspect("Hash calculated: #{hash_calculated} ::: Hash got from Discogs: #{h}")
# end)

# Unzip

[artists, labels, masters, releases]
|> Enum.filter(fn file ->
  Path.join("data", file)
  |> String.replace(".gz", "")
  |> File.exists?()
  |> Kernel.not()
end)
|> Enum.each(fn file ->
  file_path = Path.join("data", file)
  IO.inspect(file_path)
  # zcat data/discogs_20231001_artists.xml.gz > data/discogs_20231001_artists.xml
  System.cmd("gzcat", [file_path], into: File.stream!(String.replace(file_path, ".gz", "")))

  # File.stream!([:binary], 1024)
  # StreamGzip.gunzip()
end)

IO.inspect("Decompression is done")

# <artist>
#   <id>13339863</id>
#   <name>Darkside U.C.O.N.N.</name>
#   <profile></profile>
#   <data_quality>Needs Major Changes</data_quality>
#   <namevariations>
#   <name>Dark Side U.C.O.N.N.</name>
#   <name>Uconn</name>
#   </namevariations>
# </artist>

defmodule X do
  import SweetXml

  def convert_xml_to_csv(xs) do
    xs
    |> Enum.map(fn file ->
      Path.join("data", file)
      |> String.replace(".gz", "")
    end)
    |> Enum.each(fn file ->
      IO.inspect("Processing #{file}")

      file
      |> File.stream!()
      |> SweetXml.stream_tags!(:artist, discard: [:artist])
      |> Stream.map(fn {:artist, doc} ->
        doc
        |> SweetXml.xpath(~x"//artist"e, id: ~x"./id/text()"s, name: ~x"./name/text()"s)
      end)
      |> Stream.map(fn m -> "#{m.id}, \"#{m.name}\"\n" end)
      # |> Stream.map(&IO.inspect/1)
      |> Stream.into(File.stream!(String.replace(file, ".xml", ".csv")))
      |> Stream.run()
    end)
  end
end

X.convert_xml_to_csv([artists])

# xml |> xpath(~x"//artist"e, id: ~x"./id/text()"s, name: ~x"./name/text()"s)
