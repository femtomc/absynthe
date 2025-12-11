defmodule Absynthe.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Dataflow field registry - provides ETS-based storage for reactive fields.
      # Must be started before any actors that use dataflow fields.
      Absynthe.Dataflow.Registry
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Absynthe.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
