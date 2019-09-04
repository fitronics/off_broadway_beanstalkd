defmodule OffBroadwayBeanstalkd.MixProject do
  use Mix.Project

  def project do
    [
      app: :off_broadway_beanstalkd,
      version: "0.1.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:broadway, "~> 0.3.0"},
      {:beanstix, git: "https://github.com/nicksanders/beanstix.git"},
      {:credo, "~> 1.1.0", only: :dev, runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      lint: ["format --check-formatted", "credo --strict"]
    ]
  end
end
