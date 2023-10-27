Mix.install([
  {:bandit, "~> 1.0"}
])

defmodule Router do
  use Plug.Router
  plug(Plug.Logger)
  plug(:match)
  plug(:dispatch)

  get "/hello" do
    send_resp(conn, 200, "World!")
  end

  get "/" do
    send_resp(conn, 200, "Index")
  end

  match _ do
    send_resp(conn, 404, "Not found")
  end
end

bandit = {Bandit, plug: Router}
{:ok, _} = Supervisor.start_link([bandit], strategy: :one_for_one)

# unless running from IEx, sleep idenfinitely so we can serve requests
unless IEx.started?() do
  Process.sleep(:infinity)
end
