Mix.install([
  {:brotli, "~> 0.3.0"}
])

defmodule Http do
  def save_to_file(url, file_path) do
    :httpc.request(:get, {String.to_charlist(url), []}, [], stream: String.to_charlist(file_path))
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

[
  checksum,
  artists,
  labels,
  masters,
  releases
]
|> Enum.map(fn file -> Http.save_to_file("#{base_url}/#{file}", "data/#{file}") end)
