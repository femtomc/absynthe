defmodule Absynthe.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/femtomc/absynthe"

  def project do
    [
      app: :absynthe,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      consolidate_protocols: Mix.env() != :test,
      deps: deps(),

      # Hex.pm
      name: "Absynthe",
      description: description(),
      package: package(),
      docs: docs(),
      source_url: @source_url,
      homepage_url: @source_url
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Absynthe.Application, []}
    ]
  end

  defp description do
    """
    Syndicated Actor Model for Elixir - an implementation of Tony Garnock-Jones'
    actor model with Preserves data format, dataspaces, and reactive dataflow.
    """
  end

  defp package do
    [
      name: "absynthe",
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Syndicate Lang" => "https://syndicate-lang.org/",
        "Preserves Spec" => "https://preserves.gitlab.io/preserves/preserves.html"
      },
      maintainers: ["femtomc"],
      files: ~w(lib examples .formatter.exs mix.exs README.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "Absynthe",
      source_ref: "v#{@version}",
      source_url: @source_url,
      extras: ["README.md"],
      groups_for_modules: [
        Core: [
          Absynthe.Core.Actor,
          Absynthe.Core.Entity,
          Absynthe.Core.Facet,
          Absynthe.Core.Turn,
          Absynthe.Core.Ref
        ],
        Dataspace: [
          Absynthe.Dataspace.Dataspace,
          Absynthe.Dataspace.Skeleton,
          Absynthe.Dataspace.Pattern,
          Absynthe.Dataspace.Observer
        ],
        Dataflow: [
          Absynthe.Dataflow.Field,
          Absynthe.Dataflow.Registry
        ],
        Preserves: [
          Absynthe.Preserves,
          Absynthe.Preserves.Value,
          Absynthe.Preserves.Compare,
          Absynthe.Preserves.Pattern,
          Absynthe.Preserves.Encoder.Binary,
          Absynthe.Preserves.Encoder.Text,
          Absynthe.Preserves.Decoder.Binary,
          Absynthe.Preserves.Decoder.Text
        ],
        Protocol: [
          Absynthe.Protocol.Event,
          Absynthe.Assertions.Handle,
          Absynthe.Assertions.Bag
        ]
      ]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:decibel, "~> 0.2.4"}
    ]
  end
end
