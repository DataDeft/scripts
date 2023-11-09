
defmodule XmlStreamer do
  def to_elements_stream(xml_stream) do
    {:ok, partial} = Saxy.Partial.new(Release, [])

    Stream.chunk_while(xml_stream, partial, &maybe_items/2, &finalize/1)
  end

  defp maybe_items(chunk, partial) do
    with {:cont, partial} <- Saxy.Partial.parse(partial, chunk),
         {:ok, partial, items} <- fetch_items(partial) do
      if items == [], do: {:cont, partial}, else: {:cont, items, partial}
    else
      {:error, exception} when is_exception(exception) ->
        raise Saxy.ParseError.message(exception)

      {:error, reason} ->
        raise reason

      _ ->
        raise "Xml parsing error"
    end
  end

  defp finalize(partial) do
    case Saxy.Partial.terminate(partial) do
      {:ok, %{items: []}} ->
        {:cont, %{}}

      {:ok, %{items: items}} ->
        {:cont, items, %{}}

      {:error, exception} when is_exception(exception) ->
        raise Saxy.ParseError.message(exception)
    end
  end

  defp fetch_items(%{state: %{user_state: %{items: items} = user_state} = state} = partial) do
    partial = %{partial | state: %{state | user_state: %{user_state | items: []}}}
    {:ok, partial, items}
  end

  defp fetch_items(_), do: {:error, "Something wrong when processing sax events"}
end
